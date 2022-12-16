from datetime import datetime
import time
import sys


class Worker:
    def __init__(self, db_engine, max_time, expected_task_time,
                 sleep_seconds=-1):
        self.start_time = datetime.now()
        self.stop_time = self.start_time + max_time - expected_task_time
        self.db_engine = db_engine
        self.metadata = sqla.MetaData(bind=engine)
        self.tasks_table = self.metadata.tables['tasks']
        self.sleep_seconds = sleep_seconds

        # current task is associated with the instance so that failure modes
        # (i.e., on_termination) work correctly
        self.task = None

    def run_serialized_task(self, task):
        """Deserialize and run the task.
        """
        raise NotImplementedError()

    def save_results(self, results, task, trial: int):
        """Save the results object that comes from the task.

        This is likely to include two kinds of actions to ensure this:

        1. Moving files from scratch space to the shared storage area.
        2. Saving in-memory data from the results object to the shared
           storage area.
        """
        raise NotImplementedError()

    def is_failure_result(self, result) -> bool:
        """Tell whether the given result is a failure result.

        Failure results can occur if a problem happens during the
        simulation, e.g., an exception is raised. Since this may be a random
        failure, tasks that return failure results can be retried.
        """
        raise NotImplementedError()

    def select_task(self):
        ...

    def on_termination(self):
        if self.task is not None:
            self.update_task_status(self.task, TaskStatus.AVAILABLE,
                                    TaskStatus.IN_PROGRESS, allow_fail=True)

    def update_dag(self, completed_task):
        ...

    def update_task_status(self, task, status, old_status=None, *,
                           allow_fail=False):
        # this should raise an error if old_status is not the current status
        ...

    def run_one_task(self):
        while not self.task := self.select_task():
            # there are no tasks available; sleep or exit
            if self.sleep_seconds > 0:
                time.sleep(self.sleep_seconds)
            else:
                sys.exit()

        # do main work of the task
        # TODO: should this be wrapped with a try/except and always return a
        # failure result if so?
        results = self.run_serialized_task(self.task)
        self.save_results(results)

        if self.is_failure_result(result):
            # TODO logic based on number of tries -- too many failures leads
            # to error status
            self.update_task_status(self.task, TaskStatus.AVAILABLE,
                                    TaskStatus.IN_PROGRESS)
        else:
            self.update_dag(completed_task=self.task)
            self.update_task_status(self.task, TaskStatus.COMPLETED,
                                    TaskStatus.IN_PROGRESS)

        self.task = None

    def run(self):
        while datetime.now() < self.stop_time:
            self.run_one_task()
