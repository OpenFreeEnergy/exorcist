from enum import Enum
from dataclasses import dataclass
from datetime import datetime

class TaskStatus(Enum):
    BLOCKED = 0
    AVAILABLE = 1
    IN_PROGRESS = 2
    RESULTS_READY = 3
    COMPLETED = 99
    ERROR = -1


@dataclass
class Task:
    taskid: str
    taskfile: str
    last_modified: datetime
    n_retries: int

    @classmethod
    def from_db_row(row):
        ...
