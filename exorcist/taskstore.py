import abc

from typing import Generic, Callable
from .models import TaskDetails, Result

class TaskDetailsStore(abc.ABC, Generic[TaskDetails, Result]):
    """Task details storage.

    Client code must provide a storage object that conforms to this abstract
    API.
    """
    @abc.abstractmethod
    def store_task_details(self, taskid: str, task_details: TaskDetails):
        """Store the given task details.
        """
        raise NotImplementedError()

    @abc.abstractmethod
    def load_task_details(self, taskid: str) -> TaskDetails:
        """Load the task details from disk."""
        raise NotImplementedError()

    @abc.abstractmethod
    def load_task(self, taskid: str) -> Callable[[], Result]:
        raise NotImplementedError()
