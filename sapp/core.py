# Copyright (c) Haoyi Wu.
# Licensed under the MIT license.

import json
import os
import re
import shlex
import shutil
import socket
import subprocess
import warnings
from datetime import datetime
from pathlib import Path
from typing import List

from slash import Slash

from . import utils as utils
from .config import SlurmConfig, SubmitConfig


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

        # for backward compatibility, remove all clash-related settings
        if recent := data.get("recent", None):
            recent.pop("clash", None)

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
                    except IOError:
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

                # slash may block the process, resolve file first
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

                # get the host ip
                host_ip = socket.gethostbyname(socket.gethostname())

                if config.slash == "none":

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

                else:

                    # let the slash service live with the current process
                    with Slash(env_name=config.slash) as slash:
                        port = slash.service.port

                        # write the shell script
                        with open(shell_path, 'w') as f:
                            print("#!/usr/bin/bash", file = f)
                            print("", file = f)
                            print(f"export http_proxy=http://{host_ip}:{port}", file = f)
                            print(f"export https_proxy=http://{host_ip}:{port}", file = f)
                            print(f'echo $SLURM_JOB_ID > {shlex.join([jobid_path])}', file = f)
                            print(f'hostname > {shlex.join([hostname_path])}', file = f)
                            print(shlex.join(resolved_command), file = f)

                        # set env vars for tqdm
                        utils.set_screen_shape()

                        # add commands
                        # FIXME: I use os.system here because I don't know how to use
                        # subprocess to run a command while passing the SIGINT signal
                        # to the child process in an elegant way. See
                        # https://docs.python.org/3/library/subprocess.html#replacing-os-system
                        args += ["bash", str(shell_path)]
                        os.system(shlex.join(args))

            elif config.task == 2:
                args += command
                print(" ".join(args))

        elif config.task in (1, 3): # execute sbatch
            args = utils.get_command(config, tp = "sbatch", identifier=self.identifier, general_config=self.config)
            args += [""]

            if config.task == 1:

                # slash may block the process, resolve file first
                resolved_command = resolve_files(command)

                # create folder for this job
                # it should have been created in resolve_files, but we do it here for safety
                shell_folder = self.base_path / self.identifier
                shell_folder.mkdir(parents=True, exist_ok=True)

                # save the job info
                jobid_path = str((shell_folder / "SLURM_JOB_ID").absolute())
                hostname_path = str((shell_folder / "HOSTNAME").absolute())

                # get the host ip
                host_ip = socket.gethostbyname(socket.gethostname())

                if config.slash == "none":

                    args += [f'echo $SLURM_JOB_ID > {shlex.join([jobid_path])}']
                    args += [f'hostname > {shlex.join([hostname_path])}']
                    args += [shlex.join(resolved_command)]

                else:

                    # init clash
                    slash = Slash(env_name=config.slash)
                    jobname = f"__sapp_{self.identifier}__"
                    service = slash.launch(jobname)
                    port = service.port

                    args += [f"export http_proxy=http://{host_ip}:{port}"]
                    args += [f"export https_proxy=http://{host_ip}:{port}"]
                    args += [f'echo $SLURM_JOB_ID > {shlex.join([jobid_path])}']
                    args += [f'hostname > {shlex.join([hostname_path])}']
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

                # submit the job
                # we capture the output first, then print them to console
                result = subprocess.run(["sbatch", str(shell_path)], stdout=subprocess.PIPE)
                output = result.stdout.decode()

                if output:
                    print(output, end='')

                # if we launch the slash service, do some preparation
                if config.slash != "none":

                    # parse the output, we assert this is the only possible output based on
                    # https://github.com/SchedMD/slurm/blob/01cec7faa194990bc95b8a18adc1c29ac7f8733b/src/sbatch/sbatch.c#L333
                    match = re.match(r"^Submitted batch job (\d+)(?: on cluster .+)?$", output)

                    # if success, save the job id
                    if result.returncode == 0 and match:
                        jobid = match.group(1)
                        with open(jobid_path, 'w') as f:
                            f.write(jobid)

                    # if failed, stop the slash service
                    else:
                        slash.stop(jobname)

            elif config.task == 3:
                args += [shlex.join(command)]
                print("\n".join(args))


