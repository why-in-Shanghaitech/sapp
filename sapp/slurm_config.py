# Copyright (c) Haoyi Wu.
# Licensed under the MIT license.

from typing import List, Optional, Union
from dataclasses import dataclass, field
from pathlib import Path
import subprocess
import shlex
import json
import os, sys
from datetime import datetime


SAPP_FOLDER = "~/.config/sapp"

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
    def __init__(self) -> None:
        self.base_path = Path(SAPP_FOLDER).expanduser()
        self.base_path.mkdir(parents=True, exist_ok=True)
        self.identifier = datetime.now().strftime('%Y-%m-%d_%H-%M-%S')

        self.load()
    
    def add(self, config: SlurmConfig):
        self.settings.append(config)

    def remove(self, indices: List[int]):
        if indices:
            self.settings = [self.settings[i] for i in range(len(self.settings)) if i not in indices]

    def load(self):
        config_path = self.base_path / ".config"
        if config_path.exists():
            with open(config_path, 'r') as f:
                data = json.loads(f.read())
        else:
            data = {
                # sapp global config
                "config": {},

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

        # do execution
        if config.task in (0, 2): # execute srun
            args = utils.get_command(config, tp = "srun")
            args += command

            if config.task == 0:
                utils.set_screen_shape()
                os.system(shlex.join(args))
                # subprocess.run(args, shell=False) # do not raise error for KeyboardInterrupt
            elif config.task == 2:
                print(" ".join(args))
        elif config.task in (1, 3): # execute sbatch
            args = utils.get_command(config, tp = "sbatch")
            args += [""]
            args += [shlex.join(command)]

            if config.task == 1:
                # write the shell script
                shell_folder = self.base_path / self.identifier
                shell_folder.mkdir(parents=True, exist_ok=True)
                shell_path = shell_folder / "script.sh"

                with open(shell_path, 'w') as f:
                    for line in args:
                        print(line, file = f)

                os.system(shlex.join(["sbatch", str(shell_path)]))
            elif config.task == 3:
                print("\n".join(args))

        

class utils:
    """Namespace for utils."""

    @staticmethod
    def get_command(config: Union[SlurmConfig, SubmitConfig], tp: str = None):
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
            if slurm_config.gpu_type == "Any Type":
                args += [f"--gres=gpu:{slurm_config.num_gpus}"]
            else:
                args += [f"--gres=gpu:{slurm_config.gpu_type}:{slurm_config.num_gpus}"]
            args += ["-c", str(slurm_config.cpus_per_task)]
            if slurm_config.mem: args += ["--mem", slurm_config.mem]
            args += shlex.split(slurm_config.other)

            if isinstance(config, SubmitConfig):
                args += ["-t", config.time]
                if config.output: args += ["-o", config.output]
                if config.error: args += ["-e", config.error]
                if config.jobname: args += ["-J", config.jobname]

        elif tp == 'sbatch':
            args += ["#!/usr/bin/bash"]
            args += [f"#SBATCH -N {slurm_config.nodes}"]
            args += [f"#SBATCH -n {slurm_config.ntasks}"]
            args += [f"#SBATCH -p {slurm_config.partition}"]
            if slurm_config.gpu_type == "Any Type":
                args += [f"#SBATCH --gres=gpu:{slurm_config.num_gpus}"]
            else:
                args += [f"#SBATCH --gres=gpu:{slurm_config.gpu_type}:{slurm_config.num_gpus}"]
            args += [f"#SBATCH -c {slurm_config.cpus_per_task}"]
            if slurm_config.mem: args += [f"#SBATCH --mem {slurm_config.mem}"]
            if slurm_config.other: args += [f"#SBATCH {slurm_config.other}"] # TODO: what about multiple arguments?

            if isinstance(config, SubmitConfig):
                args += [f"#SBATCH -t {config.time}"]
                if config.output: args += [f"#SBATCH -o {config.output}"]
                if config.error: args += [f"#SBATCH -e {config.error}"]
                if config.jobname: args += [f"#SBATCH -J {config.jobname}"]
        
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
    