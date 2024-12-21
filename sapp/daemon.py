# Copyright (c) Haoyi Wu.
# Licensed under the MIT license.

import os
import re
import subprocess
import sys
from pathlib import Path
from typing import List

from slash.daemon import Daemon


class SappDaemon(Daemon):
    """
    The sapp daemon that manages the jobs based on the sapp identifier, which maps to a unique slurm job id.
    """

    name = "sapp"

    def launch_command(self) -> List[str]:
        """
        Get the command to launch the daemon.
        """
        return [
            sys.executable,  # the python interpreter
            "-c",
            "from sapp.daemon import SappDaemon; SappDaemon().loop({})".format(os.getpid()),
        ]

    def getid(self, job: str) -> re.Match[str]:
        """
        Get the unique identifier of the job. It will be passed to the validate method to check if the job is dead.
        If the job is beyond the control of the daemon, return None.
        """
        return re.match(r"^__sapp_(?P<pid>\d+)_(?P<identifier>.+)__$", job)

    def validate(self, match: re.Match[str]) -> bool:
        """
        Validate the existence of a job.
        """
        # if the process is alive, do not check the job
        if (Path("/proc") / match.group("pid")).exists():
            return True

        # find the job id by identifier
        jobid_path = Path("~/.config/sapp").expanduser() / match.group("identifier") / "SLURM_JOB_ID"

        # if the jobid file does not exist, the job fails to start
        if not jobid_path.exists():
            return False

        # get the jobid
        with open(jobid_path, "r") as f:
            jobid = f.read().strip()

        # check the job status
        proc = subprocess.run(
            ["squeue", "-j", str(jobid), "-O", "state", "--nohead"], stdout=subprocess.PIPE, stderr=subprocess.PIPE
        )
        return proc.returncode == 0 and proc.stdout.decode().strip()
