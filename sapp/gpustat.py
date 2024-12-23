# Copyright (c) Haoyi Wu.
# Licensed under the MIT license.

import re
import subprocess
from collections import defaultdict
from functools import partial


def parse_gres_line(line):
    """Parse the gresused line."""
    # filter out empty lines
    if line.strip() == "":
        return None

    # parse the line
    # XXX: this is not an elegent solution (cannot cover edge cases). it might
    #      be better to turn to 3rd party python slurm packages.
    status = line[:10].strip()
    gres = line[10:40].strip()
    gres_used = line[40:90].strip()
    nodelist = line[90:120].strip()
    cpus = line[120:140].strip()
    # free_mem = line[140:155].strip()
    alloc_mem = line[155:170].strip()
    total_mem = line[170:185].strip()
    partition = line[185:].strip()

    # we do not consider drain nodes
    if status not in ["idle", "mix", "alloc"]:
        return None

    # regex from https://github.com/itzsimpl/prometheus-slurm-exporter/blob/64535e24c61ea4d44795571d054c268b1ca69a35/gpus.go#L73
    # might fail when multiple types of gpus appear on the same node
    pattern = r"gpu:(\(null\)|[^:(]*):?([0-9]+)(\([^)]*\))?"
    gres_match = re.match(pattern, gres)
    gres_used_match = re.match(pattern, gres_used)

    # the gres line is not in the expected format
    if gres_match is None or gres_used_match is None:
        return None

    gpu_type = gres_match.group(1)
    gpu_total = int(gres_match.group(2))
    gpu_used = int(gres_used_match.group(2))
    gpu_avail = gpu_total - gpu_used

    # the gpu type is not specified
    if gpu_type == "(null)" or gpu_type == "":
        gpu_type = "Unknown GPU Type"

    # rule out invalid cpu info
    if cpus.count("/") != 3:
        return None
    cpu_avail = int(cpus.split("/")[1])

    # XXX: I am not sure if the memory that is available to be allocated could be calculated in this way.
    mem_avail = int(total_mem) - int(alloc_mem)

    return gpu_type, nodelist, gpu_avail, cpu_avail, mem_avail, partition


def get_card_list():
    """
    Return a dict with the key as partitions and the values as the availibility of the cards.
    The unit of the memory is MB.

    e.g.
    {
        "debug": {
            "NVIDIAA40": [
                {
                    "nodelist": "ai_gpu01",
                    "gpu": 1,
                    "cpu": 10,
                    "mem": 359477
                },
                {
                    "nodelist": "ai_gpu02",
                    "gpu": 0,
                    "cpu": 2,
                    "mem": 427480
                },
                {
                    "nodelist": "ai_gpu03",
                    "gpu": 2,
                    "cpu": 10,
                    "mem": 217510
                }
            ],
            "NVIDIATITANRTX": [
                {
                    "nodelist": "ai_gpu30",
                    "gpu": 4,
                    "cpu": 48,
                    "mem": 385415
                }
            ]
        }
    }
    """
    cmd = [
        "sinfo",
        "-N",
        "-O",
        "StateCompact:.10,Gres:.30,GresUsed:.50,NodeList:.30,CPUsState:.20,FreeMem:.15,AllocMem:.15,Memory:.15,PartitionName:.50",
        "--noheader",
    ]
    result = subprocess.run(cmd, stdout=subprocess.PIPE)

    if result.returncode != 0:
        raise RuntimeError("sinfo fails to execute. Please check if slurm is available.")

    response = result.stdout.decode("utf-8")
    resources = defaultdict(partial(defaultdict, list))
    for line in response.split("\n"):
        parse = parse_gres_line(line)

        # skip invalid lines
        if parse is None:
            continue

        gpu_type, nodelist, gpu_avail, cpu_avail, mem_avail, partition = parse
        resources[partition][gpu_type].append(
            {"nodelist": nodelist, "gpu": gpu_avail, "cpu": cpu_avail, "mem": mem_avail}
        )

    return resources
