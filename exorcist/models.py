from enum import Enum
from typing import TypeVar, Generic
import dataclasses

# generics: the actual types here depend on the client library
Result = TypeVar("Result")
TaskDetails = TypeVar("TaskDetails")

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
