"""
Script to backup Twitter data using rsync.

A lockfile ensures that this script does not run until the previous run has
finished.

"""

from __future__ import print_function

import errno
import fcntl
import glob
import os
import subprocess
import sys
import time

import configparser

config = configparser.ConfigParser()
config.read('../project.cfg')
BACKUP_PATH = config['Locations']['BACKUP_PATH']
MONGO_PREFIX = config['Prefixes']['MONGO_PREFIX']

def rsync(path=None):
    if path is None:
        path = BACKUP_PATH

    print()
    print("-----")
    subprocess.call('date')
    cmd = 'rsync --progress -zhtr *.gz *.log {0}* {1}'
    cmd = cmd.format(MONGO_PREFIX, path)
    print(cmd)
    subprocess.call(cmd, shell=True)

def main():
    with open('.lock_rsync', 'w') as f:
        try:
            fcntl.lockf(f, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except IOError, e:
            if e.errno == errno.EAGAIN:
                msg = '[{0}] rsync script already running.\n'
                msg = msg.format(time.strftime('%c'))
                sys.stderr.write(msg)
                sys.exit(-1)
            raise
        rsync()

if __name__ == '__main__':
    main()
