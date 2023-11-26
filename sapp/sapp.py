#! /usr/bin/env python
#
# Copyright (c) Haoyi Wu.
# Licensed under the MIT license.

from .forms import SlurmApplication
import sys

# for clash service
from .slurm_config import Clash
import os, shlex

def main():
    sapp = SlurmApplication(sys.argv[1:])
    sapp.run()
    sapp.process()

def spython():
    sapp = SlurmApplication(['python'] + sys.argv[1:])
    sapp.run()
    sapp.process()

def spython3():
    sapp = SlurmApplication(['python3'] + sys.argv[1:])
    sapp.run()
    sapp.process()

def clash():
    clash_ = Clash()
    if '--background' in sys.argv or '-b' in sys.argv:
        if '--background' in sys.argv:
            sys.argv.remove('--background')
        if '-b' in sys.argv:
            sys.argv.remove('-b')
        pid = clash_.runbg(['nohup', str(clash_.executable)] + sys.argv[1:])
        print("The clash process is running in the background with pid {}".format(pid))
        print("You can kill it with `kill -9 {}`".format(pid))
    else:
        os.system(shlex.join([str(clash_.executable)] + sys.argv[1:]))


if __name__ == '__main__':
    main()