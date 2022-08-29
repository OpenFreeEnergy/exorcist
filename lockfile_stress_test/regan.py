#!/usr/bin/env python

import argparse
import time
import subprocess
import shlex
from datetime import datetime

import sqlalchemy as sqla


def create_empty_database(filename):
    """Create the empty file"""
    engine = sqla.create_engine(f"sqlite:///{filename}")
    metadata = sqla.MetaData(bind=engine)
    tasks = sqla.Table('tasks', metadata,
                       sqla.Column('id', sqla.Integer),
                       sqla.Column('start', sqla.DateTime),
                       sqla.Column('acquire', sqla.DateTime))
    metadata.create_all(engine)
    return engine


def report_with_lock(task_id, lock_time, filename, lock):
    """This is the main function for reporting.

    User provides a context ``lock``. In general, tests for specific
    lockfile methods will import this method and use it directly.

    Parameters
    ----------

    """
    # this should be imported by the external scripts that use a specific
    # lock mechanism
    start = datetime.now()
    # the with statement here should be blocking until the lock is acquired
    # (or timeout error is raised)
    with lock:
        acquired = datetime.now()
        engine = sqla.create_engine(f"sqlite:///{filename}")
        metadata = sqla.MetaData()
        metadata.reflect(bind=engine)
        tasks = metadata.tables['tasks']
        insert = sqla.insert(tasks).values(id=task_id,
                                           start=start,
                                           acquire=acquired)
        with engine.connect() as conn:
            result = conn.execute(insert)

        time.sleep(lock_time)


def make_parser():
    parser = argparse.ArgumentParser()
    parser.add_argument('-n', '--taskid', type=int)
    parser.add_argument('-t', '--lock-time', type=float,
                        help="time each task holds the lockfile (seconds)")
    parser.add_argument('-f', '--file', type=str,
                        help="filename for output database")
    return parser


def main(task_id, lock_time, lock_script, filename):
    # subprocess to run lock script
    create_empty_database(filename)
    cmd = f"./{lock_script} -t {lock_time} -f {filename} -n {task_id}"
    run_cmd = shlex.split(cmd)
    result = subprocess.run(run_cmd)
    print(f"Ran task {task_id}, result code was {result.returncode}")


if __name__ == "__main__":
    parser = make_parser()
    parser.add_argument('lockscript', type=str)
    opts = parser.parse_args()
    main(opts.taskid, opts.lock_time, opts.lockscript, opts.file)

