#!/usr/bin/env python

# Use the built-in locking in sqlite (likely to fail)

from regan import report_with_lock, make_parser
import contextlib

if __name__ == "__main__":
    parser = make_parser()
    opts = parser.parse_args()
    report_with_lock(opts.taskid, opts.lock_time, opts.file,
                     contextlib.nullcontext())
