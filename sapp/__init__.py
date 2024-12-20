# Copyright (c) Haoyi Wu.
# Licensed under the MIT license.

from .sapp import main, spython, spython3

# register the SlurmDaemon
from .slash_daemon import SlurmDaemon
from slash import Slash
Slash.daemons.append(SlurmDaemon)
del Slash, SlurmDaemon
