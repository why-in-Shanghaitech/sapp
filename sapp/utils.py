# Copyright (c) Haoyi Wu.
# Licensed under the MIT license.

import os
import shlex
import shutil
import socket
import sys
import time
import warnings
from pathlib import Path
from typing import List, Union

import pexpect
import pyotp
from filelock import SoftFileLock

from .config import SlurmConfig, SubmitConfig


def resolve_identifier(s: str, identifier: str = None):
    return s.replace("%i", identifier) if identifier is not None else s


def parse_arguments(s: str) -> List[str]:
    """
    Parse the arguments into different lines for sbatch use.
    """
    args = shlex.split(s)
    lines = []
    curr = []
    for arg in args:
        if arg.startswith("-") and len(arg) == 2 or arg.startswith("--") and len(arg) > 2:
            if curr:
                lines.append(shlex.join(curr))
                curr = [arg]
            else:
                curr.append(arg)
        else:
            curr.append(arg)
    if curr:
        lines.append(shlex.join(curr))
    return lines


def get_command(
    config: Union[SlurmConfig, SubmitConfig], tp: str = None, identifier: str = None, general_config: dict = None
):
    general_config = {} if general_config is None else general_config
    slurm_config = config.slurm_config if isinstance(config, SubmitConfig) else config
    if tp is None and isinstance(config, SubmitConfig):
        tp = "srun" if config.task in (0, 2) else "sbatch"
    args = []

    if tp == "srun":
        args += ["srun"]
        args += ["-N", str(slurm_config.nodes)]
        args += ["-n", str(slurm_config.ntasks)]
        if slurm_config.disable_status:
            args += ["-X"]
        if slurm_config.unbuffered:
            args += ["-u"]
        args += ["-p", str(slurm_config.partition)]
        gpu_argname = "--gpus=" if general_config.get("gpu", False) else "--gres=gpu:"
        if slurm_config.gpu_type == "Any Type" or slurm_config.gpu_type == "Unknown GPU Type":
            args += [f"{gpu_argname}{slurm_config.num_gpus}"]
        else:
            args += [f"{gpu_argname}{slurm_config.gpu_type}:{slurm_config.num_gpus}"]
        args += ["-c", str(slurm_config.cpus_per_task)]
        if slurm_config.mem:
            args += ["--mem", slurm_config.mem]
        if slurm_config.other:
            args += shlex.split(slurm_config.other)

        if isinstance(config, SubmitConfig):
            args += ["-t", config.time]
            if config.output:
                args += ["-o", resolve_identifier(config.output, identifier)]
            if config.error:
                args += ["-e", resolve_identifier(config.error, identifier)]
            if config.jobname:
                args += ["-J", config.jobname]
            if config.mail_type:
                args += ["--mail-type", ",".join(config.mail_type)]
            if config.mail_user:
                args += ["--mail-user", config.mail_user]

    elif tp == "sbatch":
        args += ["#!/usr/bin/bash"]
        args += [f"#SBATCH -N {slurm_config.nodes}"]
        args += [f"#SBATCH -n {slurm_config.ntasks}"]
        args += [f"#SBATCH -p {slurm_config.partition}"]
        gpu_argname = "--gpus=" if general_config.get("gpu", False) else "--gres=gpu:"
        if slurm_config.gpu_type == "Any Type" or slurm_config.gpu_type == "Unknown GPU Type":
            args += [f"#SBATCH {gpu_argname}{slurm_config.num_gpus}"]
        else:
            args += [f"#SBATCH {gpu_argname}{slurm_config.gpu_type}:{slurm_config.num_gpus}"]
        args += [f"#SBATCH -c {slurm_config.cpus_per_task}"]
        if slurm_config.mem:
            args += [f"#SBATCH --mem {slurm_config.mem}"]
        if slurm_config.other:
            for line in parse_arguments(slurm_config.other):
                args += [f"#SBATCH {line}"]

        if isinstance(config, SubmitConfig):
            args += [f"#SBATCH -t {config.time}"]
            if config.output:
                args += [f"#SBATCH -o {resolve_identifier(config.output, identifier)}"]
            if config.error:
                args += [f"#SBATCH -e {resolve_identifier(config.error, identifier)}"]
            if config.jobname:
                args += [f"#SBATCH -J {config.jobname}"]
            if config.mail_type:
                args += [f"#SBATCH --mail-type {','.join(config.mail_type)}"]
            if config.mail_user:
                args += [f"#SBATCH --mail-user {config.mail_user}"]

    return args


def set_screen_shape():
    """
    Tqdm might fail on detecting screen shape. Pass the screen shape to the environment variables.
    """

    def _screen_shape_linux(fp):  # pragma: no cover
        try:
            from array import array
            from fcntl import ioctl
            from termios import TIOCGWINSZ
        except ImportError:
            return None, None
        else:
            try:
                rows, cols = array("h", ioctl(fp, TIOCGWINSZ, "\0" * 8))[:2]
                return cols, rows
            except Exception:
                try:
                    return [int(os.environ[i]) - 1 for i in ("COLUMNS", "LINES")]
                except (KeyError, ValueError):
                    return None, None

    cols, rows = _screen_shape_linux(sys.stderr)

    # Do not overwrite user environment
    if isinstance(cols, int):
        os.environ.setdefault("COLUMNS", str(cols + 1))

    if isinstance(rows, int):
        os.environ.setdefault("LINES", str(rows + 1))


def resolve_files(command: List[str], shell_folder: Path):
    """make a copy for all small (<1M) files mentioned in the command."""
    shell_folder = Path(shell_folder)
    shell_folder.mkdir(parents=True, exist_ok=True)

    _command = []
    for arg in command:
        if os.path.isfile(arg) and os.path.getsize(arg) < 1 * 1024 * 1024:
            # copy to SAPP space
            try:
                arg = shutil.copy(arg, shell_folder)
            except IOError:
                warnings.warn(
                    f"Fails to copy files in command line: {arg}. You might need to keep this file untouched till the job starts running.",
                    UserWarning,
                )

        _command.append(arg)

    # env vars for python modules
    os.environ["PYTHONPATH"] = os.environ["PATH"] + ":" + str(os.getcwd())
    return _command


def guess_ssh_port() -> int:
    """
    Guess the ssh port. This function is used to guess the ssh port on the login node.
    """
    # default ssh port
    ssh_port = 22

    # try to read the ssh port from the sshd_config
    try:
        with open("/etc/ssh/sshd_config", "r") as f:
            for line in f:
                line = line.strip()
                if line.startswith("#"):
                    continue
                if "Port" in line:
                    ssh_port = int(line.split()[1])
                    break
    except (FileNotFoundError, PermissionError):
        pass

    return ssh_port

def prepare_ssh_env(path: Path) -> Path:
    """
    Prepare password free login with ssh. This is necessary for the compute node to do port forwarding.

    Arguments:
        path: pathlib.Path
            The path to the folder where the ssh key pair is expected to be stored.

    Return:
        private_key: pathlib.Path
            The path to the private key.
    """
    private_key = Path(path) / 'id_rsa'
    public_key = Path(path) / 'id_rsa.pub'
    authorized_keys = Path('~/.ssh/authorized_keys').expanduser()

    # add a file lock to avoid race condition
    with SoftFileLock(private_key.with_suffix('.lock')):

        # check the existence of ssh key pair, and authorized keys
        if private_key.is_file() and public_key.is_file() and authorized_keys.is_file():
            with open(public_key, 'r') as f_in, open(authorized_keys, 'r') as f_out:
                for line in f_out.readlines():
                    if f_in.readline().strip() in line.strip():
                        # everything is prepared, exit
                        return private_key

        # remove existing files
        private_key.unlink(missing_ok=True)
        public_key.unlink(missing_ok=True)

        # generate the ssh key pair and write into authorized keys
        os.system(shlex.join(["ssh-keygen", "-t", "rsa", "-f", str(private_key), "-N", ""]))

        with open(public_key, 'r') as f:
            public_key_s = f.read()

        ## check if end with newline, if not, add a new line at front
        if authorized_keys.is_file():
            with open(authorized_keys, 'r') as f:
                s = f.read()
            if s != "" and not s.endswith('\n'):
                public_key_s = "\n" + public_key_s

        # append to authorized keys
        with open(authorized_keys, 'a') as f:
            f.write(public_key_s)

    return private_key

def get_ssh_command(
    path: Path,
    src_port: int,
    tgt_port: int,
    ssh_port: int = 22,
    use_pexpect: bool = False,
) -> List[str]:
    """
    Return the command for the compute node to do port forwarding to the login node.
    This function should only be called on the login node.
    """
    private_key = prepare_ssh_env(path)
    host_name, login_name = socket.gethostname(), os.getlogin()
    ssh_command = ["ssh", "-o", "StrictHostKeyChecking=no", "-N", "-f", "-L", f"{tgt_port}:localhost:{src_port}", f"{login_name}@{host_name}", "-p", str(ssh_port), "-i", str(private_key)]

    # if not use pexpect, return the command directly
    if not use_pexpect:
        return ssh_command

    # build the command
    command = ["python", "-c", f'from sapp.utils import ssh_login_with_pexpect; ssh_login_with_pexpect("{shlex.join(ssh_command)}")']

    return command

def ssh_login_with_pexpect(ssh_command: str) -> None:
    """
    Do ssh login (e.g. for port forwarding) on the compute node to the login node.
    This function should only be called on the compute node.
    pexpect is used to handle the password and otp login. However, it is known that pexpect does not work on some clusters. The reason is unknown.
    XXX: Is there a better way to do this? For example, using pxssh (no support to otp), redssh (direct implmentation of ssh) or paramiko (low level support).
    """
    timeout = 30  # TODO: allow user to control the timeout

    process = pexpect.spawn(ssh_command, timeout=1) # a fake timeout to avoid blocking
    expect_list = [
        "Verification code: ",
        "password: ",
        pexpect.EOF,
        pexpect.TIMEOUT,
    ]

    while True:
        i = process.expect(expect_list)
        if i == 0:
            # try to get the verification code through secret key
            otp_secret = None

            ## 1. find the secret key from sapp config
            if not otp_secret:
                from .core import Database
                database = Database()
                key = database.config.get("otp_secret", "")
                if isinstance(key, str) and key.strip() != "":
                    otp_secret = key.strip()

            ## 2. find the secret key from .google_authenticator
            if not otp_secret:
                path_to_totp = Path("~/.google_authenticator").expanduser()
                if path_to_totp.is_file():
                    with open(path_to_totp, 'r') as f:
                        # the first line is the secret key
                        otp_secret = f.readline().strip()

            if not otp_secret:
                raise ValueError("SSH port forwarding requires a verification code. Please set up the secret key in the general settings of SAPP.")

            # generate the verification code
            totp = pyotp.TOTP(otp_secret)

            # do not respond too fast
            time.sleep(0.1)
            process.sendline(str(totp.now()))

        elif i == 1:
            # try to get the password
            password = None

            ## 1. find the password from sapp config
            if not password:
                from .core import Database
                database = Database()
                key = database.config.get("passwd", "")
                if isinstance(key, str) and key.strip() != "":
                    password = key.strip()

            # try to get the password
            if not password:
                raise ValueError("SSH port forwarding requires a password. Please set up the password in the general settings of SAPP.")

            # do not respond too fast
            time.sleep(0.1)
            process.sendline(password)

        elif i == 2:
            break

        elif i == 3:
            timeout -= 1
            if timeout <= 0:
                process.kill(9)
                raise TimeoutError("Timeout when doing ssh port forwarding.")

    process.wait()
