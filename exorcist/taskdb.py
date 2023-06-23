import abc
import sqlalchemy as sqla

# remaining imports are for typing
from typing import Optional, Iterable
from .models import TaskStatus, Task
from os import PathLike
import networkx as nx


class NoStatusChange(Exception):
    """Raised when an attempt to change task status does not change status.

    In most cases, like when a task status is changed to "completed", if the
    task is already marked as the new status, this indicates database
    corruption.

    Raising this exception has two purposes:

    1. To enable it to be caught and allow the database corruption to be
       fixed, if possible, and exit otherwise.
    2. So that, in the event that the status that needs to be set on early
       exit (e.g., due to a sigterm send by the queuing system) is the same
       as the current status, it can be caught without error and exit
       normally.
    """


class AbstractTaskStatusDB(abc.ABCMeta):
    def __init__(self):
        self._current_task_tries = None

    @property
    def current_task_tries(self):
        return self._current_task_tries

    @current_task_tries.setter
    def current_task_tries(self, value):
        if value is not None and self._current_task_tries is not None:
            raise ...
        self._current_task_tries = value

    @abc.abstractmethod
    def add_task(self, taskid: str):
        raise NotImplementedError()

    @abc.abstractmethod
    def add_task_network(self, task_network: nx.DiGraph):
        raise NotImplementedError()

    @abc.abstractmethod
    def _check_out_task(self) -> tuple[str, int]:
        raise NotImplementedError()

    def check_out_task(self) -> str:
        """...

        This has the side effect of marking ``self.current_task_tries`` with
        the number of tries for the task that has been checked out.
        """
        taskid, tries = self._check_out_task()
        self.current_task_tries = tries
        return taskid

    @abc.abstractmethod
    def _mark_task_completed(self, taskid: str, success: bool)
        raise NotImplementedError()

    def mark_task_completed(self, taskid: str, success: bool):
        """...

        This has the side effect that ``self.current_task_tries`` is set to
        ``None``, indicating that no task is currently checked out.
        """
        self._mark_task_completed(taskid, success)
        self.current_task_tries = None


class TaskStatusDB(AbstractTaskStatusDB):
    """Database for managing execution and orchestration of tasks.
    """
    def __init__(self, engine: sqla.Engine):
        metadata = sqla.MetaData()
        metadata.reflect(engine)
        if self._is_empty_db(metadata):
            self._create_empty_db(metadata, engine)
        elif not self._is_our_db(metadata):
            raise RuntimeError(f"Database at {engine} does not seem "
                               "to be a task database")

        self.metadata = metadata
        self.engine = engine

    def __repr__(self):
        return f"TaskStatusDB(engine={self.engine})"

    @property
    def tasks_table(self):
        return self.metadata.tables['tasks']

    @property
    def dependencies_table(self):
        return self.metadata.tables['dependencies']

    @classmethod
    def from_filename(cls, filename: PathLike, *, overwrite: bool = False,
                    **kwargs):
        """Create an sqlite dialect database from a filename.

        Parameters
        ----------
        filename : os.PathLike
            Filename for the database
        overwrite: bool
            If True, all tables will be dropped from the existing database
        kwargs :
            Additional keyword arguments will be passed to
            ``sqlalchemy.create_engine``. Particularly useful for debugging
            is ``echo=True``, which will output the SQL statements being
            generated internally.
        """
        engine = sqla.create_engine(f"sqlite:///{filename}", **kwargs)
        if overwrite:
            metadata = sqla.MetaData()
            metadata.reflect(bind=engine)
            metadata.drop_all(engine)

        return cls(engine)

    @staticmethod
    def _is_empty_db(metadata: sqla.MetaData) -> bool:
        return len(metadata.tables) == 0

    @staticmethod
    def _is_our_db(metadata: sqla.MetaData) -> bool:
        # TODO: implement this: this should test whether the database we get
        # has the schema we expect, and raise an error if not
        return True

    @staticmethod
    def _create_empty_db(metadata, engine):
        """Create the tables if our database is missing them"""
        tasks_table = sqla.Table(
            "tasks",
            metadata,
            sqla.Column("taskid", sqla.String, primary_key=True),
            sqla.Column("status", sqla.Integer),
            sqla.Column("last_modified", sqla.DateTime),
            sqla.Column("tries", sqla.Integer),
        )
        deps_table = sqla.Table(
            "dependencies",
            metadata,
            sqla.Column("from", sqla.String, sqla.ForeignKey("tasks.taskid")),
            sqla.Column("to", sqla.String, sqla.ForeignKey("tasks.taskid")),
        )
        # TODO: create indices that may be needed
        metadata.create_all(bind=engine)

    def add_task(self, task: Task, requirements: Iterable[Task]):
        """Add a task to the database.

        Parameters
        ----------
        task: Task
            the task to add to the database
        requirements: Iterable[Task]
            tasks that directly block the task to be added (typically, whose
            outputs are inputs to the task)
        """
        ...

    def add_task_network(self, task_network: nx.DiGraph):
        """Add a network of tasks to the database.

        Parameters
        ----------
        task_network: nx.Digraph
            A network with :class:`.Task`\ s as nodes.
        """
        ...

    def update_task_status(
        self,
        taskid: str,
        status: TaskStatus,
        old_status: Optional[TaskStatus] = None
    ):
        """
        Parameters
        ----------
        taskid: str
            task ID of the task to update
        status: TaskStatus
            the status to change to
        old_status: TaskStatus
            the previous status

        Raises
        ------
        NoStatusChange :
            If the status does not actually change (e.g., the code is trying
            to change it to a status it already has), this is raised. This
            allows callers to catch and handle this case.
        """
        ...

    def _mark_task_completed(self, completed_taskid: str):
        """
        Update the database (including the DAG info) to show that the task
        has been completed.
        """
        ...
