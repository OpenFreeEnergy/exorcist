#!/usr/bin/env python
import argparse
import itertools
import time
import shlex
import subprocess
from datetime import datetime

import numpy as np
import pandas as pd
import sqlalchemy as sqla

from regan import create_empty_database


def make_parser():
    parser = argparse.ArgumentParser()
    parser.add_argument('lockscript', help="lock script to run", type=str)
    parser.add_argument('-f', '--file', type=str,
                        help="filename for output database")
    parser.add_argument('-N', '--nruns', help="number of runs", type=int)
    parser.add_argument('--stddev', type=float,
                        help="standard deviation of the Gaussian (seconds)")
    parser.add_argument('-t', '--lock-time', type=float,
                        help="time each task holds the lockfile (seconds)")
    return parser


def get_delays(nruns, stddev):
    """Deltas between sorted list of Gaussian process arrival times

    Parameters
    ----------
    nruns : int
        number of runs to do
    stddev : float
        standard deviation of the Gaussian process (seconds)

    Returns
    -------
    List[float] :
        deltas between successive step; first value is always 0
    """
    rng = np.random.default_rng()
    rands = rng.normal(loc=0.0, scale=stddev, size=nruns)
    deltas = [t2 - t1 for t1, t2 in itertools.pairwise(sorted(rands))]
    return [0] + deltas


def add_karras_tables(filename, lock_script, nruns, stddev, lock_time,
                      deltas):
    """Add extra tables for Karras simulation"""
    engine = sqla.create_engine(f"sqlite:///{filename}")
    metadata = sqla.MetaData()
    metadata.reflect(bind=engine)
    karras_run = sqla.Table('karras_run', metadata,
                            sqla.Column('lock_script', sqla.String),
                            sqla.Column('nruns', sqla.Integer),
                            sqla.Column('stddev', sqla.Float),
                            sqla.Column('lock_time', sqla.Float))
    metadata.create_all(engine)
    deltas_df = pd.DataFrame(enumerate(deltas), columns=["id", "delta"])
    runinfo_insert = sqla.insert(karras_run).values(lock_script=lock_script,
                                                    nruns=nruns,
                                                    stddev=stddev,
                                                    lock_time=lock_time)
    with engine.connect() as conn:
        _ = conn.execute(runinfo_insert)

    deltas_df.to_sql('deltas', engine.connect())


def main(lock_script, nruns, stddev, lock_time, filename):
    create_empty_database(filename)
    deltas = get_delays(nruns, stddev)
    procs = []
    for taskid, delay in enumerate(deltas):
        time.sleep(delay)
        cmd = f"./{lock_script} -n {taskid} -t {lock_time} -f {filename}"
        print(datetime.now(), delay, cmd)
        run_cmd = shlex.split(cmd)
        proc = subprocess.Popen(run_cmd)
        procs.append(proc)

    def any_still_running(proclist):
        for proc in proclist:
            if proc.poll() is None:
                return True
        return False

    while any_still_running(procs):
        print("waiting for still running processes")
        time.sleep(0.1)


    add_karras_tables(filename, lock_script, nruns, stddev, lock_time,
                      deltas)


if __name__ == "__main__":
    parser = make_parser()
    opts = parser.parse_args()
    main(opts.lockscript, opts.nruns, opts.stddev, opts.lock_time,
         opts.file)
