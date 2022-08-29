#!/usr/bin/env python
import argparse
import itertools
import time
import shlex
import subprocess

import numpy as np

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
    rng = np.random.default_rng()
    rands = rng.normal(loc=0.0, scale=stddev, size=nruns)
    deltas = [t2 - t1 for t1, t2 in itertools.pairwise(sorted(rands))]
    return [0] + deltas


def main(lock_script, nruns, stddev, lock_time, filename):
    create_empty_database(filename)
    deltas = get_delays(nruns, stddev)
    procs = []
    for taskid, delay in enumerate(deltas):
        time.sleep(delay)
        cmd = f"./{lock_script} -n {taskid} -t {lock_time} -f {filename}"
        run_cmd = shlex.split(cmd)
        proc = subprocess.Popen(run_cmd)
        procs.append(proc)

    def any_still_running(proclist):
        for proc in proclist:
            if proc.poll() is None:
                return True
        return False

    while any_still_running(procs):
        time.sleep(0.1)


if __name__ == "__main__":
    parser = make_parser()
    opts = parser.parse_args()
    main(opts.lockscript, opts.nruns, opts.stddev, opts.lock_time,
         opts.file)
