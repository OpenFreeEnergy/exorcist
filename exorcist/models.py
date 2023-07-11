from enum import Enum
import dataclasses

class TaskStatus(Enum):
    """
    Status of a given task.
    """
    BLOCKED = 0
    AVAILABLE = 1
    IN_PROGRESS = 2
    # RESULTS_READY = 3
    COMPLETED = 99
    TOO_MANY_RETRIES = -2
    ERROR = -1
