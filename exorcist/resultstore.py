import abc

from typing import Generic
from .models import Result


class ResultStore(abc.ABC, Generic[Result]):
    """Result storage.

    Client code must provide a storage object that conforms to this abstract
    API.
    """
    @abc.abstractmethod
    def is_failure_result(self, result: Result) -> bool:
        """Test whether this result represents a failed run.

        This allows failures (e.g., raising exceptions) to be treated as
        first-class results, which can then be introspected by the users.
        """
        raise NotImplementedError()

    @abc.abstractmethod
    def store_result(self, result: Result, retry: int = 0):
        """Store a result to permanent storage.
        """
        raise NotImplementedError()
