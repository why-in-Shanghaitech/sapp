#! /usr/bin/env python
#
# Copyright (c) Haoyi Wu.
# Licensed under the MIT license.

from .forms import SlurmApplication
import sys

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


if __name__ == '__main__':
    main()