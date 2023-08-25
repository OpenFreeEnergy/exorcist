import logging
import pathlib
import uuid


class UUIDFileHandler(logging.FileHandler):
    """
    Logging FileHandler variant that uses UUID filenames within a directory.

    Most parameters are the same as in the ``logging.FileHandler`` class,
    see https://docs.python.org/3/library/logging.handlers.html#filehandler

    Parameters
    ----------
    directory : PathLike
        directory where log files will be written; this logger will have a
        filename in the format "{uuid}.log"
    encoding : str
        file encoding
    delay : bool
        whether to delay creation of the log file until the first ``emit``.
    errors :
        how to handle encoding errors
    """
    def __init__(self, directory, encoding=None, delay=False, errors=None):
        filename = pathlib.Path(directory) / f"{uuid.uuid1().hex}.log"
        super().__init__(filename, mode='a', encoding=encoding, delay=delay,
                         errors=errors)
