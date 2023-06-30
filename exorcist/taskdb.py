import abc
import sqlalchemy as sqla
import networkx as nx
from .models import TaskStatus

# remaining imports are for typing
from typing import Optional, Iterable
from os import PathLike

def sqlite_fk_pragma(dbapi_conn, conn_record):
    cursor = dbapi_conn.cursor()
    cursor.execute("PRAGMA foreign_keys=ON")
    cursor.close()


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


class AbstractTaskStatusDB(abc.ABC):
    @abc.abstractmethod
    def add_task(self, taskid: str, requirements: Iterable[str],
                 max_tries: int):
        """Add a task to the database.

        Parameters
        ----------
        taskid: str
            the taskid to add to the database
        requirements: Iterable[str]
            taskids that directly block the task to be added (typically,
            whose outputs are inputs to the task)
        """
        raise NotImplementedError()

    @abc.abstractmethod
    def add_task_network(self, task_network: nx.DiGraph, max_tries: int):
        """Add a network of tasks to the database.

        Parameters
        ----------
        task_network: nx.Digraph
            A network with taskid strings as nodes.
        """
        raise NotImplementedError()

    @abc.abstractmethod
    def check_out_task(self) -> str:
        """
        Select a task to be run.

        This should include any internal updates to the task status database
        to indicate that the given task has been checked out by a worker.

        Returns
        -------
        str :
            The taskid of the task to run
        """
        raise NotImplementedError()

    @abc.abstractmethod
    def mark_task_aborted_incomplete(self, taskid: str):
        """
        Update the database when a task fails to complete.

        This may be caused by, e.g., a walltime limit being hit.

        Parameters
        ----------
        taskid: str
            the taskid of the failed task
        """
        raise NotImplementedError()

    @abc.abstractmethod
    def mark_task_completed(self, taskid: str, success: bool):
        """
        Update the database when a task has completed.

        Parameters
        ----------
        taskid: str
            the taskd of the completed task
        success: bool
            True if the task completed successfully, False if there was a
            failure
        """
        raise NotImplementedError()


class TaskStatusDB(AbstractTaskStatusDB):
    """Database for managing execution and orchestration of tasks.

    This implementation is built on SQLAlchemy. For simple usage, the
    recommendation is to use the :method:`.from_filename` method to create
    this object, rather than its ``__init__``. The ``__init__`` method takes
    a SQLAlchemy engine, which provides much more flexibility in choice of
    backend.
    """
    def __init__(self, engine: sqla.Engine):
        if (
            engine.name == "sqlite"
            and not sqla.event.contains(engine, "connect", sqlite_fk_pragma)
        ):
            sqla.event.listen(engine, "connect", sqlite_fk_pragma)

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
        sqla.event.listen(engine, "connect", sqlite_fk_pragma)
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
            sqla.Column("max_tries", sqla.Integer),
        )
        deps_table = sqla.Table(
            "dependencies",
            metadata,
            sqla.Column("from", sqla.String, sqla.ForeignKey("tasks.taskid")),
            sqla.Column("to", sqla.String, sqla.ForeignKey("tasks.taskid")),
        )
        # TODO: create indices that may be needed for performance
        metadata.create_all(bind=engine)

    @staticmethod
    def _get_task_and_dep_data(taskid: str, requirements: Iterable[str],
                               max_tries: int):
        stat = TaskStatus.BLOCKED if requirements else TaskStatus.AVAILABLE
        task_data = {
            'taskid': taskid,
            'status': stat.value,
            'last_modified': None,
            'tries': 0,
            'max_tries': max_tries
        }

        deps_data = [
            {'from': req, 'to': taskid}
            for req in requirements
        ]
        return [task_data], deps_data

    def _insert_task_and_deps_data(self, task_data, deps_data):
        task_ins = sqla.insert(self.tasks_table).values(task_data)
        deps_ins = sqla.insert(self.dependencies_table).values(deps_data)

        with self.engine.begin() as conn:
            res1 = conn.execute(task_ins)
            if deps_data:  # don't insert on empty deps
                res2 = conn.execute(deps_ins)

    def add_task(self, taskid: str, requirements: Iterable[str],
                 max_tries: int):
        """Add a task to the database.

        Parameters
        ----------
        taskid: str
            the task to add to the database
        requirements: Iterable[str]
            taskids that directly block the task to be added (typically,
            whose outputs are inputs to the task)
        """
        task_data, deps = self._get_task_and_dep_data(taskid, requirements,
                                                      max_tries)
        self._insert_task_and_deps_data(task_data, deps)

    def add_task_network(self, taskid_network: nx.DiGraph, max_tries: int):
        """Add a network of tasks to the database.

        Parameters
        ----------
        taskid_network: nx.Digraph
            A network with taskids (str) as nodes. Edges in this graph
            follow the direction of time/flow of information: from earlier
            tasks to later tasks; from requirements to subsequent.
        """
        all_data = [
            self._get_task_and_dep_data(node, taskid_network.pred[node],
                                        max_tries)
            for node in nx.topological_sort(taskid_network)
        ]
        tasklists, deplists = zip(*all_data)
        tasks = sum(tasklists, [])
        deps = sum(deplists, [])
        self._insert_task_and_deps_data(tasks, deps)

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

    def check_out_task(self):
        ...

    def mark_task_aborted_incomplete(self, taskid: str):
        ...

    def mark_task_completed(self, completed_taskid: str):
        """
        Update the database (including the DAG info) to show that the task
        has been completed.
        """
        ...
