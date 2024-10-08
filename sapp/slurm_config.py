# Copyright (c) Haoyi Wu.
# Licensed under the MIT license.

from typing import List, Tuple, Optional, Union
from dataclasses import dataclass, field
from pathlib import Path
import shlex, shutil
import os, sys, subprocess
import json
from datetime import datetime
import warnings
import requests
import socket
import pexpect
import pyotp
import yaml
import time
import gzip, tempfile
from tqdm import tqdm
from .filelock import FileLock
from .bindport import FreePort
import random


@dataclass
class SlurmConfig:
    name: Optional[str] = field(
        default=None,
        metadata={
            "help": "Config name. The only identifier to store in the database.",
            "desc": "None for temporary use."
        }
    )
    nodes: int = field(
        default=1,
        metadata={
            "help": "Request that a minimum of minnodes nodes be allocated to this job."
        }
    )
    ntasks: int = field(
        default=1,
        metadata={
            "help": "Specify the number of tasks to run. Unless you know its meaning, do not change."
        }
    )
    disable_status: bool = field(
        default=True,
        metadata={
            "help": "Disable the display of task status when srun receives a single SIGINT (Ctrl-C). Only useful for srun."
        }
    )
    unbuffered: bool = field(
        default=True,
        metadata={
            "help": "Always flush the outputs to console. Only useful for srun."
        }
    )
    partition: str = field(
        default=None,
        metadata={
            "help": "Request a specific partition for the resource allocation."
        }
    )
    gpu_type: str = field(
        default=None,
        metadata={
            "help": "GPU type for allocation."
        }
    )
    num_gpus: int = field(
        default=1,
        metadata={
            "help": "Number of GPUs to request."
        }
    )
    cpus_per_task: int = field(
        default=2,
        metadata={
            "help": "Request that ncpus be allocated per process. This may be useful if the job is multithreaded."
        }
    )
    mem: Optional[str] = field(
        default=None,
        metadata={
            "help": "(Optional) Specify the real memory required per node. E.g. 40G."
        }
    )
    other: Optional[str] = field(
        default=None,
        metadata={
            "help": "(Optional) Other command line arguments, such as '--exclude ai_gpu02,ai_gpu04'."
        }
    )

@dataclass
class SubmitConfig:
    slurm_config: SlurmConfig = field(
        default=None
    )
    jobname: Optional[str] = field(
        default=None,
        metadata={
            "help": "(Optional) Job name for slurm. Will appear in squeue."
        }
    )
    clash: int = field(
        default="0-01:00:00",
        metadata={
            "help": "Limit on the total run time of the job allocation. E.g. 0-01:00:00"
        }
    )
    time: str = field(
        default="0-01:00:00",
        metadata={
            "help": "Limit on the total run time of the job allocation. E.g. 0-01:00:00"
        }
    )
    output: str = field(
        default=None,
        metadata={
            "help": "(Optional) The output filename. use %j for job id and %x for job name. You may want to leave it blank for srun."
        }
    )
    error: str = field(
        default=None,
        metadata={
            "help": "(Optional) The stderr filename. use %j for job id and %x for job name. You may want to leave it blank for srun."
        }
    )
    mail_type: Optional[List[str]] = field(
        default=None,
        metadata={
            "help": "(Optional) Specify the type of mail notification requested.",
            "options": [
                "NONE", "BEGIN", "END", "FAIL", "REQUEUE", "ALL", "INVALID_DEPEND", "STAGE_OUT", "TIME_LIMIT", "TIME_LIMIT_90", "TIME_LIMIT_80", "TIME_LIMIT_50", "ARRAY_TASKS"
            ]
        }
    )
    mail_user: Optional[str] = field(
        default=None,
        metadata={
            "help": "(Optional) Specify the email address to send notification to."
        }
    )
    task: int = field(
        default=None,
        metadata={
            "help": "Task to execute.",
            "options": [
                "Submit job with srun", "Submit job with sbatch", "Print srun command", "Print sbatch header"
            ]
        }
    )


class Database:

    SAPP_FOLDER = "~/.config/sapp"

    def __init__(self) -> None:
        self.base_path = Path(self.SAPP_FOLDER).expanduser()
        self.base_path.mkdir(parents=True, exist_ok=True)
        self.identifier = datetime.now().strftime('%Y-%m-%d_%H-%M-%S')

        self.load()
    
    def add(self, config: SlurmConfig):
        self.settings.append(config)

    def remove(self, indices: List[int]) -> int:
        """return the number of settings deleted."""
        if indices:
            before = len(self.settings)
            self.settings = [self.settings[i] for i in range(len(self.settings)) if i not in indices]
            after = len(self.settings)
            return before - after
        return 0

    def load(self):
        config_path = self.base_path / ".config"
        if config_path.exists():
            with open(config_path, 'r') as f:
                data = json.loads(f.read())
        else:
            data = {
                # sapp global config
                "config": {
                    "log_space": 0
                },

                # user settings
                "settings": [],

                # the most recent user setting
                "recent": None
            }
        
        # initialization
        self.config: dict = data.get("config", {})
        self.settings: List[SlurmConfig] = [SlurmConfig(**s) for s in data.get("settings", [])]
        self.recent: SubmitConfig = None
        if data.get("recent", None):
            self.recent = SubmitConfig(SlurmConfig(**data["recent"].pop("slurm_config", {})), **data["recent"])

        # clean databse
        log_space = self.config.get("log_space", 0)
        candidates = [p for p in self.base_path.iterdir() if p.is_dir()]
        if log_space > 0 and len(candidates) > log_space: # remove old folders
            to_remove = sorted(candidates)[:-log_space]
            for d in to_remove:
                shutil.rmtree(d, ignore_errors=True)


    def dump(self):
        data = {
            "config": self.config,
            "settings": [s.__dict__ for s in self.settings],
            "recent": {
                "slurm_config": self.recent.slurm_config.__dict__,
                **{
                    k: getattr(self.recent, k)
                    for k in self.recent.__dict__.keys()
                    if k != "slurm_config"
                }
            }
        }

        config_path = self.base_path / ".config"
        with open(config_path, 'w') as f:
            f.write(json.dumps(data, indent=4))

    def execute(self, command: List[str], config: SubmitConfig):
        self.recent = config
        self.dump() # dump befure execution

        def resolve_files(command: List[str]):
            """make a copy for all small (<1M) files mentioned in the command."""
            shell_folder = self.base_path / self.identifier / "data"
            shell_folder.mkdir(parents=True, exist_ok=True)

            _command = []
            for arg in command:
                if os.path.isfile(arg) and os.path.getsize(arg) < 1 * 1024 * 1024:

                    # copy to SAPP space
                    try:
                        arg = shutil.copy(arg, shell_folder)
                    except IOError as e:
                        warnings.warn(f"Fails to copy files in command line: {arg}. You might need to keep this file untouched till the job starts running.", UserWarning)
                    
                _command.append(arg)
            
            # env vars for python modules
            os.environ['PYTHONPATH'] = os.environ['PATH'] + ':' + str(os.getcwd())
            return _command
        
        # if the user does not want to cache the files, resolve_files will do nothing
        if not self.config.get("cache", True):
            resolve_files = lambda x: x

        # do execution
        if config.task in (0, 2): # execute srun
            args = utils.get_command(config, tp = "srun", identifier=self.identifier, general_config=self.config)

            if config.task == 0:

                # get_service may block the process, resolve file first
                resolved_command = resolve_files(command)

                # create folder for this job
                # it should have been created in resolve_files, but we do it here for safety
                shell_folder = self.base_path / self.identifier
                shell_folder.mkdir(parents=True, exist_ok=True)
                # the shell script
                shell_path = shell_folder / "script.sh"

                # save the job info
                jobid_path = str((shell_folder / "SLURM_JOB_ID").absolute())
                hostname_path = str((shell_folder / "HOSTNAME").absolute())

                if config.clash == -1:

                    # write the shell script
                    with open(shell_path, 'w') as f:
                        print("#!/usr/bin/bash", file = f)
                        print("", file = f)
                        print(f'echo $SLURM_JOB_ID > {shlex.join([jobid_path])}', file = f)
                        print(f'hostname > {shlex.join([hostname_path])}', file = f)
                        print(shlex.join(resolved_command), file = f)
                    
                    # set env vars for tqdm
                    utils.set_screen_shape()

                    # add commands
                    args += ["bash", str(shell_path)]
                    os.system(shlex.join(args))

                elif config.clash == 0:

                    # init clash
                    clash = Clash()
                    pid, port = clash.get_service(self.identifier, self.config.get("clash_config_file", None))
                    tgt_port = random.randint(30000, 40000) # impossible to find free port on compute node

                    # write the shell script
                    with open(shell_path, 'w') as f:
                        print("#!/usr/bin/bash", file = f)
                        print("", file = f)
                        print(f"export http_proxy=http://127.0.0.1:{tgt_port}", file = f)
                        print(f"export https_proxy=http://127.0.0.1:{tgt_port}", file = f)
                        print(f'echo $SLURM_JOB_ID > {shlex.join([jobid_path])}', file = f)
                        print(f'hostname > {shlex.join([hostname_path])}', file = f)
                        print(shlex.join(clash.get_ssh_command(port, tgt_port)), file = f)
                        print(shlex.join(resolved_command), file = f)
                    
                    # set env vars for tqdm
                    utils.set_screen_shape()

                    # add commands
                    args += ["bash", str(shell_path)]
                    os.system(shlex.join(args))

                    clash.release_service(self.identifier)
                    # subprocess.run(args, shell=False) # will raise error when KeyboardInterrupt

                else:

                    clash = Clash()
                    tgt_port = random.randint(30000, 40000)

                    with open(shell_path, 'w') as f:
                        print("#!/usr/bin/bash", file = f)
                        print("", file = f)
                        print(f"export http_proxy=http://127.0.0.1:{tgt_port}", file = f)
                        print(f"export https_proxy=http://127.0.0.1:{tgt_port}", file = f)
                        print(f'echo $SLURM_JOB_ID > {shlex.join([jobid_path])}', file = f)
                        print(f'hostname > {shlex.join([hostname_path])}', file = f)
                        print(shlex.join(clash.get_ssh_command(config.clash, tgt_port)), file = f)
                        print(shlex.join(resolved_command), file = f)
                    
                    # set env vars for tqdm
                    utils.set_screen_shape()

                    # add commands
                    args += ["bash", str(shell_path)]
                    os.system(shlex.join(args))

                
            elif config.task == 2:
                args += command
                print(" ".join(args))
        
        elif config.task in (1, 3): # execute sbatch
            args = utils.get_command(config, tp = "sbatch", identifier=self.identifier, general_config=self.config)
            args += [""]

            if config.task == 1:

                # get_service may block the process, resolve file first
                resolved_command = resolve_files(command)

                # create folder for this job
                # it should have been created in resolve_files, but we do it here for safety
                shell_folder = self.base_path / self.identifier
                shell_folder.mkdir(parents=True, exist_ok=True)
                    
                # save the job info
                jobid_path = str((shell_folder / "SLURM_JOB_ID").absolute())
                hostname_path = str((shell_folder / "HOSTNAME").absolute())

                if config.clash == -1:

                    args += [f'echo $SLURM_JOB_ID > {shlex.join([jobid_path])}']
                    args += [f'hostname > {shlex.join([hostname_path])}']
                    args += [shlex.join(resolved_command)]

                elif config.clash == 0:

                    # init clash
                    clash = Clash()
                    pid, port = clash.get_service(self.identifier, self.config.get("clash_config_file", None))
                    host_name, login_name = socket.gethostname(), os.getlogin()
                    tgt_port = random.randint(30000, 40000) # impossible to find free port on compute node

                    args += [f"export http_proxy=http://127.0.0.1:{tgt_port}"]
                    args += [f"export https_proxy=http://127.0.0.1:{tgt_port}"]
                    args += [f'echo $SLURM_JOB_ID > {shlex.join([jobid_path])}']
                    args += [f'hostname > {shlex.join([hostname_path])}']
                    args += [shlex.join(clash.get_ssh_command(port, tgt_port))]
                    args += [shlex.join(resolved_command)]
                    args += [shlex.join(['python', '-c', f'from sapp.slurm_config import Clash; Clash.release_service_compute("{self.identifier}", "{host_name}", "{login_name}")'])] # release

                else:

                    # init clash
                    clash = Clash()
                    tgt_port = random.randint(30000, 40000) # impossible to find free port on compute node

                    args += [f"export http_proxy=http://127.0.0.1:{tgt_port}"]
                    args += [f"export https_proxy=http://127.0.0.1:{tgt_port}"]
                    args += [f'echo $SLURM_JOB_ID > {shlex.join([jobid_path])}']
                    args += [f'hostname > {shlex.join([hostname_path])}']
                    args += [shlex.join(clash.get_ssh_command(config.clash, tgt_port))]
                    args += [shlex.join(resolved_command)]
                
                # write the shell script
                shell_path = shell_folder / "script.sh"

                with open(shell_path, 'w') as f:
                    for line in args:
                        print(line, file = f)

                # print the output and error file path to console
                if config.output:
                    print("Stdout filepath:", utils.resolve_identifier(config.output, self.identifier))
                if config.error:
                    print("Stderr filepath:", utils.resolve_identifier(config.error, self.identifier))

                os.system(shlex.join(["sbatch", str(shell_path)]))
                
            elif config.task == 3:
                args += [shlex.join(command)]
                print("\n".join(args))



class utils:
    """Namespace for utils."""
    
    @staticmethod
    def resolve_identifier(s: str, identifier: str = None):
        return s.replace("%i", identifier) if identifier is not None else s
    
    @staticmethod
    def parse_arguments(s: str) -> List[str]:
        """
        Parse the arguments into different lines for sbatch use.
        """
        args = shlex.split(s)
        lines = []
        curr = []
        for arg in args:
            if arg.startswith("-") and len(arg) == 2 or \
                arg.startswith("--") and len(arg) > 2:
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

    @staticmethod
    def get_command(config: Union[SlurmConfig, SubmitConfig], tp: str = None, identifier: str = None, general_config: dict = None):
        general_config = {} if general_config is None else general_config
        slurm_config = config.slurm_config if isinstance(config, SubmitConfig) else config
        if tp is None and isinstance(config, SubmitConfig):
            tp = 'srun' if config.task in (0, 2) else 'sbatch'
        args = []

        if tp == 'srun':
            args += ["srun"]
            args += ["-N", str(slurm_config.nodes)]
            args += ["-n", str(slurm_config.ntasks)]
            if slurm_config.disable_status: args += ["-X"]
            if slurm_config.unbuffered: args += ["-u"]
            args += ["-p", str(slurm_config.partition)]
            gpu_argname = "--gpus=" if general_config.get("gpu", False) else "--gres=gpu:"
            if slurm_config.gpu_type == "Any Type" or slurm_config.gpu_type == "Unknown GPU Type":
                args += [f"{gpu_argname}{slurm_config.num_gpus}"]
            else:
                args += [f"{gpu_argname}{slurm_config.gpu_type}:{slurm_config.num_gpus}"]
            args += ["-c", str(slurm_config.cpus_per_task)]
            if slurm_config.mem: args += ["--mem", slurm_config.mem]
            args += shlex.split(slurm_config.other)

            if isinstance(config, SubmitConfig):
                args += ["-t", config.time]
                if config.output: args += ["-o", utils.resolve_identifier(config.output, identifier)]
                if config.error: args += ["-e", utils.resolve_identifier(config.error, identifier)]
                if config.jobname: args += ["-J", config.jobname]
                if config.mail_type: args += ["--mail-type", ",".join(config.mail_type)]
                if config.mail_user: args += ["--mail-user", config.mail_user]

        elif tp == 'sbatch':
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
            if slurm_config.mem: args += [f"#SBATCH --mem {slurm_config.mem}"]
            if slurm_config.other:
                for line in utils.parse_arguments(slurm_config.other):
                    args += [f"#SBATCH {line}"]

            if isinstance(config, SubmitConfig):
                args += [f"#SBATCH -t {config.time}"]
                if config.output: args += [f"#SBATCH -o {utils.resolve_identifier(config.output, identifier)}"]
                if config.error: args += [f"#SBATCH -e {utils.resolve_identifier(config.error, identifier)}"]
                if config.jobname: args += [f"#SBATCH -J {config.jobname}"]
                if config.mail_type: args += [f"#SBATCH --mail-type {','.join(config.mail_type)}"]
                if config.mail_user: args += [f"#SBATCH --mail-user {config.mail_user}"]
        
        return args

    @staticmethod
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
                    rows, cols = array('h', ioctl(fp, TIOCGWINSZ, '\0' * 8))[:2]
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
    
    @staticmethod
    def get_slurm_version() -> Tuple[int, int, int]:
        """Get the version of slurm installed."""
        r = os.popen("sinfo --version")
        version = r.read().strip()
        r.close()
        return tuple(map(int, version.split()[1].split('.')))
    
    @staticmethod
    def download_file(
        url: List[str],
        path: Union[str, Path],
        desc: str = None,
        write_callback = None,
    ):
        """
        Download a file from the internet. If the file already exists, it will skip the download.
        If the url is blocked, then try the next one.

        write_callback should be a function with the source and target file descriptors as input.
        """
        if not isinstance(url, list):
            url = [url]
        
        if isinstance(path, str):
            path = Path(path)
        
        if path.exists():
            return

        for idx, u in enumerate(url):
            try:
                r = requests.get(u, stream = True)
                total = int(r.headers.get('Content-Length', 0)) // 1024
                with tempfile.TemporaryFile("w+b") as tmp:
                    # download to tmp dir
                    for chunk in tqdm(r.iter_content(chunk_size = 1024), desc=desc, total=total, unit='KB', leave=False):
                        if chunk:
                            tmp.write(chunk)
                    tmp.seek(0)
                    # move to home
                    with open(path, "wb") as f:
                        if write_callback:
                            write_callback(tmp, f)
                        else:
                            f.write(tmp.read())
                break
            except requests.exceptions.RequestException:
                print(f"Failed to download {u}. Retrying ({idx + 1}/{len(url)})...")
        else:
            raise requests.exceptions.RequestException("All urls are blocked.")


class Clash:

    EXEC_FOLDER = '~/.cache/sapp'

    def __init__(self, use_custom: int = 0) -> None:
        """
        Initialize the clash service.

        Arguments:
            - use_custom: Use the custom clash setting with default path (~/.config/clash).
                          Carry the customized port.
        """
        self.exec_folder = Path(self.EXEC_FOLDER).expanduser()
        self.use_custom = use_custom
        self.executable # auto download first

    @property
    def executable(self) -> Path:
        """
        Return the path to the clash executable. Download if not found.
        """

        exec_path = self.exec_folder / "mihomo-v1.18.1"

        if not exec_path.exists(): # download and cache

            print("Preparing web environment. Please wait, it could take a few minutes...")
            self.exec_folder.mkdir(parents=True, exist_ok=True)

            # Use mihomo to support more protocols
            utils.download_file(
                url = [
                    "https://github.com/MetaCubeX/mihomo/releases/download/v1.18.1/mihomo-linux-amd64-v1.18.1.gz",
                    "https://mirror.ghproxy.com/https://github.com/MetaCubeX/mihomo/releases/download/v1.18.1/mihomo-linux-amd64-v1.18.1.gz",
                    "https://gitee.com/jiang-zhida/mihomo/releases/download/v1.16.0/clash.meta-linux-amd64-v1.16.0.gz" # the version on gitee is older
                ],
                path = exec_path,
                desc = "Download Clash",
                write_callback = lambda src, tgt: tgt.write(gzip.decompress(src.read()))
            )
            
            exec_path.chmod(mode = 484) # rwxr--r--

        mmdb_path = self.exec_folder / "mihomo" / "geoip.metadb"

        if not mmdb_path.exists():

            mmdb_path.parent.mkdir(parents=True, exist_ok=True)

            # use fastly instead of cdn
            utils.download_file(
                url = [
                    "https://fastly.jsdelivr.net/gh/MetaCubeX/meta-rules-dat@release/geoip.metadb",
                    "https://github.com/MetaCubeX/meta-rules-dat/releases/download/latest/geoip.metadb",
                    "https://testingcf.jsdelivr.net/gh/MetaCubeX/meta-rules-dat@release/geoip.metadb"
                ],
                path = mmdb_path,
                desc = "Download geoip"
            )
        
        # prepare for custom usage of clash
        default_mmdb_path = Path("~/.config/mihomo/geoip.metadb").expanduser()

        if not default_mmdb_path.exists():
            default_mmdb_path.parent.mkdir(parents=True, exist_ok=True)
            shutil.copy(mmdb_path, default_mmdb_path)
            
        return exec_path
    
    
    def get_service(self, identifier: str, config_path: str = None) -> Tuple[int, int]:
        """
        Return a clash service with a tuple (pid, port).
        Register the current application.

        Arguments:
            - identifier: The only identifier of the application to use the service.
        """
        logger = self.exec_folder / "logger.json"
        hostname = socket.gethostname()

        # check if service already exist
        with FileLock(logger) as lock:
            data = {}
            key = hostname + ('-custom' if self.use_custom else '')

            if logger.exists():
                # read the service data, and append the identifier.
                with open(logger, "r") as f:
                    string = f.read()

                if string:
                    data = json.loads(string)
            
            status = data.get(key, {"pid": -1, "port": -1, "jobs": []})

            # check if the service is still running.
            if key in data and (Path('/proc') / str(status.get("pid", -1))).exists():
                # is alive
                pid, port = status["pid"], status["port"]
                status["jobs"].append(identifier)

            else:
                # create a new service
                if self.use_custom:
                    pid = self.runbg(['nohup', str(self.executable)])
                    port = self.use_custom
                    
                else:
                    ## Step 1. Get a free port
                    freeport = FreePort()
                    port = freeport.port

                    ## Step 2. Initialize the config file
                    config_folder = self.exec_folder / "mihomo"
                    config_folder.mkdir(parents=True, exist_ok=True)

                    if config_path is None or config_path == "":

                        # write an empty config file
                        with open(config_folder / "config.yaml", 'w') as f:
                            print(f"mixed-port: {port}", file=f)
                    
                    else:

                        # check if the config file exists
                        config_path = Path(config_path).expanduser()
                        if not config_path.exists():
                            raise FileNotFoundError(f"Config file {config_path} does not exist. Please fix it in the general settings.")

                        # read from the config file
                        with open(config_path, 'r') as f:
                            clash_config = yaml.safe_load(f)
                        
                        # properly set the port
                        if 'port' in clash_config:
                            del clash_config['port']
                        if 'socks-port' in clash_config:
                            del clash_config['socks-port']
                        if 'redir-port' in clash_config:
                            del clash_config['redir-port']
                        if 'tproxy-port' in clash_config:
                            del clash_config['tproxy-port']

                        clash_config['mixed-port'] = int(port)

                        # write to the config file
                        with open(config_folder / "config.yaml", 'w') as f:
                            yaml.dump(clash_config, f)
                    
                    ## Step 3. Start service
                    pid = self.runbg(['nohup', str(self.executable), "-d", str(config_folder)])

                    freeport.release()

                status = {"pid": pid, "port": port, "jobs": [identifier]}

            # update data
            with open(logger, "w") as f:
                data[key] = status
                f.write(json.dumps(data))

        return pid, port


    def release_service(self, identifier: str) -> None:
        """
        Stop the clash service if no application is running.
        """
        logger = self.exec_folder / "logger.json"
        hostname = socket.gethostname()

        with FileLock(logger) as lock:
            data = {}
            key = hostname + ('-custom' if self.use_custom else '')

            # read data
            if logger.exists():
                with open(logger, "r") as f:
                    string = f.read()

                if string:
                    data = json.loads(string)
            
            status = data.get(key, {"pid": -1, "port": -1, "jobs": []})

            # check if the service is still running.
            if key in data and (Path('/proc') / str(status.get("pid", -1))).exists():
                # is alive
                if identifier in status.get("jobs", []):
                    status["jobs"].remove(identifier)

                    # double check squeue is not empty
                    r = os.popen(f'squeue --noheader {"" if utils.get_slurm_version() < (20,) else "--me "}-o "%.18i %.2t"')
                    result = r.read()
                    r.close()

                    # remove the lines with status CG
                    is_empty = True
                    for line in result.split('\n'):
                        if line.strip() == "":
                            continue
                        if len(line) >= 21 and line[18:21].strip() == 'CG':
                            continue
                        is_empty = False

                    if status["jobs"] and not is_empty:
                        with open(logger, "w") as f:
                            data[key] = status
                            f.write(json.dumps(data))
                    else:
                        # shut down service
                        self.runbg(["kill", "-9", str(status["pid"])])
                        with open(logger, "w") as f:
                            data.pop(key)
                            f.write(json.dumps(data))
                else:
                    # it is strange that the process is not logged
                    pass
            else:
                with open(logger, "w") as f:
                    data.pop(key, None) # safe even if key is not in data
                    f.write(json.dumps(data))

    def prepare_ssh_env(self) -> None:
        """
        Prepare password free login with ssh. This is necessary for the compute node to do port forwarding.
        """
        private_key = self.exec_folder / 'id_rsa'
        public_key = self.exec_folder / 'id_rsa.pub'
        authorized_keys = Path('~/.ssh/authorized_keys').expanduser()

        # add a file lock to avoid race condition
        with FileLock(private_key) as lock:

            # check the existence of ssh key pair, and authorized keys
            if private_key.is_file() and public_key.is_file() and authorized_keys.is_file():
                with open(public_key, 'r') as f_in, open(authorized_keys, 'r') as f_out:
                    for line in f_out.readlines():
                        if f_in.readline().strip() in line.strip():
                            # everything is prepared, exit
                            return
                        
            # remove existing files
            private_key.unlink(missing_ok=True)
            public_key.unlink(missing_ok=True)

            # generate the ssh key pair and write into authorized keys
            os.system(shlex.join(["ssh-keygen", "-f", str(private_key), "-N", ""]))

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
    
    def get_ssh_command(self, src_port: int, tgt_port: int) -> List[str]:
        """
        Return the command for the compute node to do port forwarding to the login node.
        This function should only be called on the login node.
        """
        self.prepare_ssh_env()
        private_key = self.exec_folder / 'id_rsa'
        host_name, login_name = socket.gethostname(), os.getlogin()
        ssh_command = ["ssh", "-o", "StrictHostKeyChecking=no", "-N", "-f", "-L", f"{tgt_port}:localhost:{src_port}", f"{login_name}@{host_name}", "-i", str(private_key)]
        ssh_command = shlex.join(ssh_command)
        command = ["python", "-c", f'from sapp.slurm_config import Clash; Clash.ssh_login("{ssh_command}")']
        return command
    
    @staticmethod
    def ssh_login(ssh_command: str) -> None:
        """
        Do ssh login (e.g. for port forwarding) on the compute node to the login node.
        This function should only be called on the compute node.
        Starting from sapp 0.4.5, ssh login is done through python codes to support password and otp login.
        XXX: Is there a better way to do this? For example, using paramiko.
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
                secret_key = None

                ## 1. find the secret key from sapp config
                if not secret_key:
                    database = Database()
                    key = database.config.get("otp_secret", "")
                    if isinstance(key, str) and key.strip() != "":
                        secret_key = key.strip()

                ## 2. find the secret key from .google_authenticator
                if not secret_key:
                    path_to_totp = Path("~/.google_authenticator").expanduser()
                    if path_to_totp.is_file():
                        with open(path_to_totp, 'r') as f:
                            # the first line is the secret key
                            secret_key = f.readline().strip()

                if not secret_key:
                    raise ValueError("SSH port forwarding requires a verification code. Please set up the secret key in the general settings of SAPP.")
                
                # generate the verification code
                totp = pyotp.TOTP(secret_key)

                # do not respond too fast
                time.sleep(0.1)
                process.sendline(str(totp.now()))

            elif i == 1:
                # try to get the password
                password = None

                ## 1. find the password from sapp config
                if not password:
                    database = Database()
                    key = database.config.get("passwd", "")
                    if isinstance(key, str) and key.strip() != "":
                        password = key.strip()

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


    @staticmethod
    def runbg(command: List[str]) -> int:
        """
        Run command and return a pid.
        ref: https://stackoverflow.com/questions/6011235
        """
        p = subprocess.Popen(command,
            stdout=open('/dev/null', 'w'),
            stderr=open('/dev/null', 'w'),
            preexec_fn=os.setpgrp
        )
        return p.pid
    
    @classmethod
    def release_service_compute(cls, identifier: str, host_name: str, login_name: str, use_custom: int = 0) -> None:
        """
        On compute node we need to stop the service on the login node through ssh.
        Require calling `prepare_ssh_env` first on the login node.
        """
        exec_folder = Path(cls.EXEC_FOLDER).expanduser()
        logger = exec_folder / "logger.json"

        with FileLock(logger) as lock:
            data = {}
            key = host_name + ('-custom' if use_custom else '')

            # read data
            if logger.exists():
                with open(logger, "r") as f:
                    string = f.read()

                if string:
                    data = json.loads(string)
            
            status = data.get(key, {"pid": -1, "port": -1, "jobs": []})

            # since on compute node, we cannot check if the service is still running.
            # this is fine: we check the existence in `get_service`.
            if key in data:
                # data valid
                if identifier in status.get("jobs", []):
                    status["jobs"].remove(identifier)

                    # double check squeue is not empty
                    r = os.popen(f'squeue --noheader {"" if utils.get_slurm_version() < (20,) else "--me "}-o "%.18i %.2t"')
                    result = r.read()
                    r.close()

                    # remove the lines with status CG and current job
                    is_empty = True
                    this_jobid = os.environ.get('SLURM_JOB_ID', None)
                    for line in result.split('\n'):
                        if line.strip() == "":
                            continue
                        if len(line) > 18 and line[:18].strip() == this_jobid:
                            continue
                        if len(line) >= 21 and line[18:21].strip() == 'CG':
                            continue
                        is_empty = False

                    if status["jobs"] and not is_empty:
                        with open(logger, "w") as f:
                            data[key] = status
                            f.write(json.dumps(data))
                    else:
                        # shut down service
                        private_key = exec_folder / 'id_rsa'
                        command = ["ssh", "-o", "StrictHostKeyChecking=no", f"{login_name}@{host_name}", "-i", str(private_key), "kill", "-9", str(status["pid"])]
                        cls.ssh_login(shlex.join(command)) # have to wait till finish, which will introduce an overhead.
                        
                        with open(logger, "w") as f:
                            data.pop(key)
                            f.write(json.dumps(data))
                else:
                    # it is strange that the process is not logged
                    pass