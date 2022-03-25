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
    line = line.strip()
    m = re.match(r'gpu\:([\w\-\(\)]+)\:(\d+)\(IDX\:([\d\-\,N/A]+)\)', line)
    if not m: return None

    gpu = m.group(1)
    used = int(m.group(2))
    
    ## available gpus
    s = m.group(3).strip()
    avail = 0
    if s != 'N/A':
        for period in s.split(','):
            if '-' not in period:
                avail += 1
            else:
                start, end = period.split('-')
                avail += int(end) - int(start) + 1

    return gpu, avail - used


def get_card_list():
    """
    Return a dict with the key as partitions and the values as the availibility of the cards.

    e.g.
    {
        "debug": {
            "NVIDIAA40": 1,
            "NVIDIATITANRTX": 0
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
        part_status = defaultdict(int)
        response = run_cammand(['sinfo', '-O', 'GresUsed:.50', '-p', partition])
        for line in response.split('\n'):
            parse = parse_gres_line(line)
            if parse is not None:
                gpu, avail = parse
                part_status[gpu] += avail
        resources[partition] = part_status
    
    return resources

if __name__ == '__main__':
    from pprint import pprint
    pprint(get_card_list())