# Copyright (c) Haoyi Wu.
# Licensed under the MIT license.

from dataclasses import dataclass, field
from typing import List, Optional


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
    slash: int = field(
        default="none",
        metadata={
            "help": "Slash service to use. If none, do not use slash service.",
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
