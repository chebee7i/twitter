"""
Script to zip archived tweet files.

A lockfile ensures that this script does not run until the previous run
has finished.

"""

import errno
import fcntl
import glob
import subprocess
import sys
import time

def gzip():
    for filename in glob.glob('tweets.*'):
        if filename.endswith('gz'):
            continue
        subprocess.call(['gzip', filename])

def main():
    with open('.lock_gzip', 'w') as f:
        try:
            fcntl.lockf(f, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except IOError, e:
            if e.errno == errno.EAGAIN:
                msg = '[{0}] Gzip script already running.\n'
                msg = msg.format(time.strftime('%c'))
                sys.stderr.write(msg)
                sys.exit(-1)
            raise
        else:
            msg = '[{0}] Starting gzip script.\n'.format(time.strftime('%c'))
            sys.stderr.write(msg)
        gzip()

if __name__ == '__main__':
    main()
