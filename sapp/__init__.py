# Copyright (c) Haoyi Wu.
# Licensed under the MIT license.

from .sapp import main, spython, spython3

# register the SappDaemon
from .daemon import SappDaemon
from slash import Slash
Slash.daemons.append(SappDaemon)
del Slash, SappDaemon
