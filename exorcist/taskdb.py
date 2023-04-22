import sqlalchemy as sqla
import networkx as nx
from .models import TaskStatus
from datetime import datetime

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


class TaskStatusDB:
    """Database for managing execution and orchestration of tasks.
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
        )
        deps_table = sqla.Table(
            "dependencies",
            metadata,
            sqla.Column("from", sqla.String, sqla.ForeignKey("tasks.taskid")),
            sqla.Column("to", sqla.String, sqla.ForeignKey("tasks.taskid")),
            sqla.Column("blocking", sqla.Boolean),
        )
        # TODO: create indices that may be needed
        metadata.create_all(bind=engine)

    @staticmethod
    def _get_task_and_dep_data(taskid: str, requirements: Iterable[str]):
        stat = TaskStatus.BLOCKED if requirements else TaskStatus.AVAILABLE
        task_data = {
            'taskid': taskid,
            'status': stat.value,
            'last_modified': None,
            'tries': 0,
        }

        deps_data = [
            {'from': req, 'to': taskid, 'blocking': True}
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

    def add_task(self, taskid: str, requirements: Iterable[str]):
        """Add a task to the database.

        Parameters
        ----------
        taskid: str
            the task to add to the database
        requirements: Iterable[str]
            taskids that directly block the task to be added (typically,
            whose outputs are inputs to the task)
        """
        task_data, deps = self._get_task_and_dep_data(taskid, requirements)
        self._insert_task_and_deps_data(task_data, deps)

    def add_task_network(self, taskid_network: nx.DiGraph):
        """Add a network of tasks to the database.

        Parameters
        ----------
        taskid_network: nx.Digraph
            A network with taskids (str) as nodes. Edges in this graph
            follow the direction of time/flow of information: from earlier
            tasks to later tasks; from requirements to subsequent.
        """
        all_data = [
            self._get_task_and_dep_data(node, taskid_network.pred[node])
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
            If no task matched the criteria (``taskid`` and, if given,
            ``old_status``) this error is raised. This allows callers to
            handle this when possible (e.g., to retry with a different task)
        """
        stmt = (
            sqla.update(self.tasks_table)
            .where(self.tasks_table.c.taskid == taskid)
        )
        if old_status is not None:
            stmt = stmt.where(self.tasks_table.c.status == old_status.value)

        stmt = stmt.values(status=status.value, last_modified=datetime.now())
        # TODO: consider whether we either want to (a) optionally pass a
        # connection here; (b) have a function that just returns the stmt
        # and have it used elsewhere
        with self.engine.begin() as conn:
            result = conn.execute(stmt)

        if result.rowcount > 1: # -no-cov-
            raise RuntimeError("Database seems to have more than 1 row with"
                               f"taskid '{taskid}'. This should not happen.")
        elif result.rowcount == 0:
            raise NoStatusChange(f"Task '{taskid}' could not change from "
                                 f"{old_status} to {status}")

    def mark_task_completed(self, completed_taskid: str):
        """
        Update the database (including the DAG info) to show that the task
        has been completed.
        """
        # TODO: This implementation is temporary.  One we have tests for
        # this in place, we can come back and refactor to move more logic to
        # SQL.
        self.update_task_status(completed_taskid, TaskStatus.COMPLETED,
                                TaskStatus.IN_PROGRESS)
        with self.engine.begin() as conn:
            # 1. UPDATE all dependency rows where from==taskid to mark these
            #    as no longer blocking; RETURNING 'to'
            # 2. QUERY to find which of the resulting tasks (resultid) have
            #    no rows in dependencies where to==resultid and
            #    blocking==True. These are the tasks that are now unblocked.
            # 3. UPDATE tasks table to mark these tasks as available
            ...

    def update_dependencies_to_match_tasks(self):
        """
        """
        # 1. UPDATE rows in dependencies where blocking==True and where the
        #    taskid in 'from' is marked COMPLETED is task table so that they
        #    are now blocking=False; RETURNING 'to'
        # 2. QUERY to find which of the resulting tasks (resultid) have no
        #    no rows in dependencies where to==resultid and
        #    blocking==True. These are the tasks that are now unblocked.
        # 3. UPDATE tasks table to mark these tasks as available

        # NOTE: steps 2 and 3 are the same as above; step 1 is just the
        # difference between doing this for all tasks and doing for one.
        # Maybe this functions hsould be called from `mark_task_completed`?
