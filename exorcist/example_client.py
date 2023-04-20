"""
Simple example of implementing Exorcist in client code.

This is useful both to illustrate how client code for Exocist can be
written, as well as to be used in our test suite.
"""


import dataclasses
import pathlib
import json
import pickle

from functools import partial

from .resultstore import ResultStore
from .taskstore import TaskDetailsStore

from typing import Callable

@dataclasses.dataclass
class ExampleResult:
    """Result class.

    This doesn't have to be a user-defined class; for example, it could be a
    dict with an expected structure.
    """
    label: str
    main_result: float | str
    is_failure: bool


@dataclasses.dataclass
class ExampleTaskDetails:
    label: str
    input_result_labels: dict[str, str]
    """keys are variable names, values are the label where result is stored
    """
    task_func: Callable

    def _extract_main_result(self, resultfile):
        with open(resultfile, mode='r') as f:
            result = ExampleResult(**json.load(f))

        return result.main_result

    def run_task(self, directory):
        inputs = {
            key: self._extract_main_result(directory / f"{inp}_result.json")
            for key, inp in self.input_result_labels.items()
        }
        try:
            main_result = self.task_func(**inputs)
            is_failure = False
        except Exception as e:
            main_result = str(e)
            is_failure = True

        return ExampleResult(
            label=self.label,
            main_result=main_result,
            is_failure=is_failure
        )


class ExampleResultStore(ResultStore):
    """Example of a ResultStore

    This stores :class:`.ExampleResult`\ s as JSON files in a given
    directory.
    """
    def __init__(self, directory):
        self.directory = directory

    def is_failure_result(self, result: ExampleResult) -> bool:
        return result.is_failure

    def store_result(self, result: ExampleResult, retry: int = 0):
        # the idea here is that there is only ever one successful result for
        # a given task, but there may be many failures -- we label the
        # failures by trial numbers. This way we save both successes and
        # failures.
        if self.is_failure_result(result):
            path = self.directory / f"{result.label}_result_{retry}.json"
        else:
            path = self.directory / f"{result.label}_result.json"

        with open(path,  mode='w') as f:
            f.write(json.dumps(dataclasses.asdict(result)))


class ExampleTaskDetailsStore(TaskDetailsStore):
    """Example of a TaskDetailsStore

    """
    def __init__(self, directory):
        self.directory = directory

    def store_task_details(self, taskid: str,
                           task_details: ExampleTaskDetails):
        with open(self.directory / f"{taskid}.p", mode='wb') as f:
            pickle.dump(task_details, f)

    def load_task_details(self, taskid: str) -> ExampleTaskDetails:
        with open(self.directory / f"{taskid}.p", mode='rb') as f:
            task_details = pickle.load(f)

        return task_details

    def run_task(self, task_details: ExampleTaskDetails) -> ExampleResult:
        return task_details.run_task(directory=self.directory)
