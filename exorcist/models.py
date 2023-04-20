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
    RESULTS_READY = 3
    COMPLETED = 99
    TOO_MANY_RETRIES = -2
    ERROR = -1


# TODO: it isn't entirely clear to me that this is needed, or that this is
# the right way to do it. but I wanted to capture the way to handle typing
# of something like this
@dataclasses.dataclass
class Task(Generic[TaskDetails]):
    """Generic to contain taskid and the client-specific TaskDetails.

    """
    taskid: str
    task_details: TaskDetails
