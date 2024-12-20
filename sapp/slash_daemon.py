# Copyright (c) Haoyi Wu.
# Licensed under the MIT license.

from typing import List
import os
import sys
import re
import subprocess
from pathlib import Path
from slash.daemon import Daemon

class SlurmDaemon(Daemon):
    """
    The slurm daemon that manages the jobs based on the slurm job id.
    """
    name = 'slurm'

    def launch_command(self) -> List[str]:
        """
        Get the command to launch the daemon.
        """
        return [
            sys.executable, # the python interpreter
            "-c",
            "from sapp.slash_daemon import SlurmDaemon; SlurmDaemon().loop({})".format(
                os.getpid()
            ),
        ]

    def getid(self, job: str) -> str:
        """
        Get the unique identifier of the job. It will be passed to the validate method to check if the job is dead.
        If the job is beyond the control of the daemon, return None.
        """
        match = re.match(r"^__sapp_(?P<identifier>.+)__$", job)
        return None if not match else match.group("identifier")

    def validate(self, jid: str) -> bool:
        """
        Validate the existence of a job.
        """
        # find the job id by identifier
        jobid_path = Path("~/.config/sapp").expanduser() / jid / "SLURM_JOB_ID"

        # assume the job is not registered
        # FIXME: give it a 30 seconds retry, the registration should be very fast
        if not jobid_path.exists():
            return True

        # get the jobid
        with open(jobid_path, "r") as f:
            jid = f.read().strip()

        proc = subprocess.run(["squeue", "-j", str(jid), "-O", "state", "--nohead"], stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        return proc.returncode == 0 and proc.stdout.decode().strip()
