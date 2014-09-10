"""
Generates a possible crontab for continuous data collection.

Uses the current directory and Python executable to fix parameters.

The three items are:
    1) stream.py : Responsible for writing tweets to file.
    2) zipper.py : Responsible for zipping archived tweet files.
    3) rsync.py : Responsible for backing up archives and log files.

Each script takes precautions to make sure it does not run again
if another run is still going from the previous cron iteration.

"""
from __future__ import print_function

import os
import sys

CURDIR = os.path.abspath(os.path.curdir)
PYTHON = sys.executable

lines = [
    "* * * * * cd {curdir} && {python} stream.py >> stream.log 2>&1",
    "15 4,12,20 * * * cd {curdir} && {python} zipper.py >> zipper.log 2>&1",
    "0 6,14,22 * * * cd {curdir} && {python} rsync.py >> rsync.log 2>&1"
]

print("\n".join(lines).format(curdir=CURDIR, python=PYTHON))
