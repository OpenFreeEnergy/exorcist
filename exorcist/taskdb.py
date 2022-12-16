import sqlalchemy as sqla


def create_task_db(engine):
    """
    Create an empty task database with the provided engine.
    """
    ...


def load_task_db(engine):
    ...



def _ensure_uri(filename: str):
    """If this is a filename, convert to a URI. If URI, leave as-is"""
    ...



class TaskDB:
    def __init__(self, engine, mode):
        ...

    @classmethod
    def from_sqlite(cls, filename: str, mode: str):
        uri = _ensure_uri(filename)
        engine = ...
        return cls(engine, mode=mode)

    def _create_empty_db(self):
        ...

    def _load_existing_db(self):
        ...

    def _load_tasks(self):
        ...

