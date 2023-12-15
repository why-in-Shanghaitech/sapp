# Copyright (c) Haoyi Wu.
# Licensed under the MIT license.

from collections import defaultdict
import subprocess
import re

def run_cammand(cmd, retry = 1):
    for _ in range(retry):
        c = subprocess.run(cmd, stdout=subprocess.PIPE)
        if c.returncode == 0:
            return c.stdout.decode('utf-8')
    raise RuntimeError("cammand fails to execute: {}".format(cmd if isinstance(cmd, str) else ' '.join(cmd)))

def parse_gres_line(line):
    """Parse the gresused line."""
    # XXX: this is not an elegent solution (cannot cover edge cases). it might
    #      be better to turn to 3rd party python slurm packages.
    status = line[:10].strip()
    gres = line[10:40].strip()
    gres_used = line[40:90].strip()

    # we do not consider drain nodes
    if status not in ['idle', 'mix', 'alloc']:
        return None

    # regex from https://github.com/itzsimpl/prometheus-slurm-exporter/blob/64535e24c61ea4d44795571d054c268b1ca69a35/gpus.go#L73
    # might fail when multiple types of gpus appear on the same node
    pattern = r'gpu:(\(null\)|[^:(]*):?([0-9]+)(\([^)]*\))?'
    gres_match = re.match(pattern, gres)
    gres_used_match = re.match(pattern, gres_used)

    # the gres line is not in the expected format
    if gres_match is None or gres_used_match is None:
        return None

    gpu = gres_match.group(1)
    avail = int(gres_match.group(2))
    used = int(gres_used_match.group(2))

    # the gpu type is not specified
    if gpu == '(null)' or gpu == '':
        gpu = 'Unknown GPU Type'

    return gpu, avail - used

def get_card_list():
    """
    Return a dict with the key as partitions and the values as the availibility of the cards.

    e.g.
    {
        "debug": {
            "NVIDIAA40": [1, 0, 0],
            "NVIDIATITANRTX": [0, 0]
        }
    }
    """
    ## Step 1: get all the partitions
    response = run_cammand(['sinfo', '-O', 'PartitionName'])

    partitions = []
    for line in response.split('\n'):
        line = line.strip()
        if line not in ("", "PARTITION"):
            partitions.append(line)
    
    ## Step 2: get the gpu status for each partition
    resources = {}
    for partition in partitions:
        part_status = defaultdict(list)
        response = run_cammand(['sinfo', '-N', '-O', 'StateCompact:.10,Gres:.30,GresUsed:.50', '-p', partition, '--noheader'])
        for line in response.split('\n'):
            parse = parse_gres_line(line)
            if parse is not None:
                gpu, avail = parse
                part_status[gpu].append(avail)
        resources[partition] = part_status
    
    return resources

if __name__ == '__main__':
    from pprint import pprint
    pprint(get_card_list())