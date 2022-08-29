#!/usr/bin/env python
from regan import report_with_lock, make_parser

from fasteners import InterProcessReaderWriterLock

if __name__ == "__main__":
    parser = make_parser()
    opts = parser.parse_args()
    lock_context = InterProcessReaderWriterLock(opts.file).write_lock()
    report_with_lock(opts.taskid, opts.lock_time, opts.file, lock_context)
