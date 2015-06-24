#! -*- encoding: utf-8 -*-
"""
Some simple helper scripts to aid in distributed job management.

"""

from multiprocessing import Process
from itertools import islice, product, izip_longest
import sqlite3
import datetime
import sys
import time

import numpy as np

#dbname = 'fullsupport_temp.db'
#conn = sqlite3.connect(dbname)
#c = conn.cursor()
#c.execute("""
#    CREATE TABLE IF NOT EXISTS machines (n int, k int, id int);
#""")

FREE, RESERVED, BUSY, DONE = 'free', 'reserved', 'busy', 'done'

# https://docs.python.org/2/library/itertools.html#recipes
def grouper(iterable, n, fillvalue=None):
    "Collect data into fixed-length chunks or blocks"
    # grouper('ABCDEFG', 3, 'x') --> ABC DEF Gxx
    args = [iter(iterable)] * n
    return izip_longest(fillvalue=fillvalue, *args)

class JobManager(object):
    """
    Simple job manager across many processes.

    """
    def __init__(self, filename, nProc=1):
        """
        Parameters
        ----------
        filename : str
            The db to use for job management.

        """
        conn = sqlite3.connect(filename)
        with conn:
            conn.execute("""
                CREATE TABLE IF NOT EXISTS jobs (job int unique, status text);
            """)
        self.filename = filename
        self.conn = conn
        self.nProc = nProc
        self.processes = []
        self.stagger = None

    def insert_job(self, job_id):
        """
        Adds a job.

        """
        row = (job_id, FREE)
        with self.conn as conn:
            statement = 'INSERT OR IGNORE INTO jobs VALUES (?, ?)'
            conn.execute(statement, row)

    def reserve_job(self, job_id, insert=True):
        """
        Reserve a particular job.

        """
        # First make sure the job exists in the database.
        if insert:
            self.insert_job(job_id)

        # Now make sure it is FREE.
        with self.conn as conn:
            statement = 'SELECT status from jobs where job = ?'
            status = conn.execute(statement, [job_id]).fetchone()
            if status is None:
                raise Exception('Unknown job: {0}'.format(i))
            else:
                status = status[0]
            if status == FREE:
                # Cannot call update_status(). This might screw up the transaction
                # that we entered via the context manager.
                statement = """INSERT OR REPLACE INTO jobs(job, status)
                               VALUES(?, ?)"""
                conn.execute(statement, (job_id, RESERVED))
                reserved = True
            else:
                reserved = False
        return reserved, status

    def update_status(self, job_id, status):
        with self.conn as conn:
            statement = "INSERT OR REPLACE INTO jobs(job, status) VALUES(?, ?)"
            conn.execute(statement, (job_id, status))

    def mark_free(self, job_id):
        self.update_status(job_id, FREE)

    def mark_busy(self, job_id):
        self.update_status(job_id, BUSY)

    def mark_done(self, job_id):
        self.update_status(job_id, DONE)

    def mark_busy_as_free(self):
        """
        Mark all busy sources as free.

        This is sometimes necessary when jobs failed abruptly.

        """
        with self.conn as conn:
            statement = """UPDATE jobs SET status = ? where status = ?"""
            conn.execute(statement, [FREE, BUSY])

    def free_all(self):
        with self.conn as conn:
            statement = """UPDATE jobs SET status = ?"""
            conn.execute(statement, [FREE])

    def count(self, status):
        with self.conn as conn:
            c = conn.execute("SELECT count(*) from jobs where status = ?", [status])
            c = c.fetchone()[0]
            return c

    def busy_count(self):
        return self.count(BUSY)

    def find_free(self):
        with self.conn as conn:
            c = conn.execute("SELECT job from jobs where status = ?", [FREE])
            c = [x[0] for x in c.fetchall()]
            c.sort()
            return c

    def main(self, arg=None):
        import sys
        if arg is None and len(sys.argv) >= 2:
            arg = sys.argv[1]

        if arg:
            if arg == 'go':
                try:
                    self.parent()
                except KeyboardInterrupt:
                    # Interrupt is passed to subprocesses.
                    pass

            elif arg == 'free':
                print("Marking all busy jobs as free.")
                self.mark_busy_as_free()

            elif arg == 'clear':
                print("Marking all jobs as free ")
                self.free_all()
        else:
            print("Busy count: {}".format(self.count(BUSY)))
            print("Free count: {}".format(self.count(FREE)))
            print("Done count: {}".format(self.count(DONE)))

    def parent(self):
        """
        User-implemented function to launch processes.

        """
        free = self.find_free()
        while len(free) > 0:
            if self.busy_count() >= self.nProc:
                time.sleep(30)
                continue
            free = self.find_free()
            if len(free):
                self.launch_child(job_id=free[0], args=None)

    def child(self, job_id, args):

        # Job has been marked busy. It will be freed if there are any uncaught
        # exceptions here. If will be marked done if this finishes.

        print("\tJob ID: {0}".format(job_id))
        print("\tArgs: {0}".format(args))
        time.sleep(20)

    def _child(self, job_id, args):
        # Establish a new connection for each child.
        self.conn = sqlite3.connect(self.filename)

        start = datetime.datetime.now()
        print("\nStart {0}:  \t{1}".format(job_id, start))
        sys.stdout.flush()

        try:
            ### The work ###
            self.child(job_id, args)
        except:
            print("\n\nFREE: {0} (after exception)".format(job_id))
            self.mark_free(job_id)
            raise
        else:
            print("DONE: {0}".format(job_id))
            self.mark_done(job_id)
        finally:
            end = datetime.datetime.now()
            print("Finish {0}: \t{1}".format(job_id, end))
            print("Elapsed {0}:\t{1}".format(job_id, end - start))
            sys.stdout.flush()

    def launch_child(self, job_id, args, wait=60, insert=True):
        # Reserve this chunk, if possible.
        stagger = self.stagger or 3
        try:
            reserved, status = self.reserve_job(job_id)
            if not reserved:
                print("Job {0}: {1}".format(job_id, status))
            else:
                print("\nRESERVED: {0}".format(job_id))
                while True:
                    # Wait for an open slot.
                    time.sleep(stagger)
                    if self.busy_count() >= self.nProc:
                        print("WAITING: {0}".format(job_id))
                        time.sleep(wait)
                        continue
                    else:
                        # Launch the process.
                        print("MARKED BUSY: {0}".format(job_id))
                        self.mark_busy(job_id)
                        p = Process(target=self._child,
                                    args=(job_id, args))
                        p.start()
                        self.processes.append(p)
                        break
        except:
            print("\nError before launching child: {0}".format(job_id))
            self.mark_free(job_id)
            raise

def run_test():
    jm = JobManager('/tmp/test.db', nProc=5)
    jm.main()

if __name__ == '__main__':
    run_test()
