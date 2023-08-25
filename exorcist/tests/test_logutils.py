import logging

from exorcist.logutils import UUIDFileHandler

def test_uuid_file_handler_emit(tmp_path):
    assert len(list(tmp_path.glob("*.log"))) == 0

    handler = UUIDFileHandler(directory=tmp_path)
    message = "message to write to log file"

    logger = logging.getLogger(f"{__name__}.test_uuid_file_handler_emit")

    logger.addHandler(handler)
    logger.setLevel(logging.DEBUG)
    logger.info(message)

    logger.setLevel(logging.NOTSET)
    logger.removeHandler(handler)

    logfiles = list(tmp_path.glob("*.log"))
    assert len(logfiles) == 1
    filename = logfiles[0]
    assert handler.baseFilename == str(filename)
    with open(filename, mode='r') as f:
        contents = f.read()

    assert message in contents
