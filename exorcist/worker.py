from contextmanagers import contextmanager

import logging
_logger = logging.getLogger(__name__)


class TimeRemainingContinueCondition:
    def __init__(self, max_time, expected_task_time):
        self.max_time = max_time
        self.expected_task_time = expected_task_time
        self.start_time = datetime.now()
        self.wall_time = self.start_time + max_time
        self.stop_time = self.start_time + max_time - expected_task_time

    def __call__(self):
        now = datetime.now()
        _logger.info(f"Estimated time remaining: {now - self.wall_time}")
        should_continue = now < self.stop_time
        _logger.info(f"Enough time to run another task? {should_continue}")
        return should_continue


class EmptyQueueSleep:
    def __init__(self, seconds):
        self.seconds = seconds

    def __call__(self):
        _logger.info(f"Sleeping for {self.sleep_seconds} seconds to see if "
                     "new tasks become available.")
        time.sleep(self.seconds)


class EmptyQueueExit:
    # only a class to keep API with things that might need internal state
    def __call__(self):
        _logger.info("No tasks available. Exiting.")
        exit()


@contextmanager
def start_stop_logging(label, logger=_logger, level=logging.INFO):
    logger.log(f"=START= {label}", level=level)
    yield
    logger.log("f=STOP= {label}", level=level)


class Worker:
    def __init__(
        self,
        taskstatusdb: TaskStatusDB,
        taskdetailsdb: TaskDetailsStore,
        resultsdb: ResultStore,
        continue_conditions: Iterable[Callable[[], bool]],
        empty_queue_behavior: Callable[[], None],
    ):
        self.taskstatusdb = taskstatusdb
        self.taskdetailsdb = taskdetailsdb
        self.resultsdb = resultsdb
        self.continue_conditions = continue_conditions
        self.empty_queue_behavior = empty_queue_behavior
        self.task = None

    def on_termination(self):
        with start_stop_logging("termination cleanup",
                                level=logging.CRITICAL):
            if self.task is not None:
                try:
                    self.update_task_status(self.taskid,
                                            TaskStatus.AVAILABLE,
                                            TaskStatus.IN_PROGRESS)
                except NoStatusChange:
                    pass  # this is fine on termination

    def select_task(self):
        ...

    def run_one_task(self):
        with start_stop_logging("selecting task"):
            while not (taskrow := self.select_task()):
                self.empty_queue_behavior()

            self.taskid, _, _, retry = taskrow

        _logger.info(f"Selected taskid {self.taskid}")

        with start_stop_logging("loading task details"):
            details = self.taskdetailsdb.load_task_details(taskid)

        with start_stop_logging("running task"):
            result = self.taskdetailsdb.run_task(details)

        with start_stop_logging("saving results"):
            self.resultsdb.save_result(result, retry)

        if self.resultsdb.is_failure_result(result):
            with start_stop_logging("marking failure result available"):
                self.taskstatusdb.update_task_status(self.taskid,
                                                     TaskStatus.AVAILABLE,
                                                     TaskStatus.IN_PROGRESS)
        else:
            with start_stop_logging("marking task completed"):
                self.taskstatusdb.mark_task_completed(task.taskid)

        self.task = None

    def should_get_another_task(self):
        return all(cond() for cond in self.continue_conditions)

    def run(self):
        while self.should_get_another_task():
            self.run_one_task()

        _logger.info("Exiting normally due to continue conditions")
