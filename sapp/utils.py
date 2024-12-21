# Copyright (c) Haoyi Wu.
# Licensed under the MIT license.

import os
import shlex
import subprocess
import sys
from typing import List, Tuple, Union

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
        if slurm_config.other: args += shlex.split(slurm_config.other)

        if isinstance(config, SubmitConfig):
            args += ["-t", config.time]
            if config.output: args += ["-o", resolve_identifier(config.output, identifier)]
            if config.error: args += ["-e", resolve_identifier(config.error, identifier)]
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
            for line in parse_arguments(slurm_config.other):
                args += [f"#SBATCH {line}"]

        if isinstance(config, SubmitConfig):
            args += [f"#SBATCH -t {config.time}"]
            if config.output: args += [f"#SBATCH -o {resolve_identifier(config.output, identifier)}"]
            if config.error: args += [f"#SBATCH -e {resolve_identifier(config.error, identifier)}"]
            if config.jobname: args += [f"#SBATCH -J {config.jobname}"]
            if config.mail_type: args += [f"#SBATCH --mail-type {','.join(config.mail_type)}"]
            if config.mail_user: args += [f"#SBATCH --mail-user {config.mail_user}"]

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
