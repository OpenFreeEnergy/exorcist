import abc
import sqlalchemy as sqla
import networkx as nx
from .models import TaskStatus
from datetime import datetime
import logging

# imports for typing
from typing import Optional, Iterable, Union
from os import PathLike
from sqlalchemy.sql.roles import StatementRole as SQLStatement

_logger = logging.getLogger(__name__)


def _sqlite_fk_pragma(dbapi_conn, conn_record):
    """Event listener function for foreign keys in sqlite

    By default, SQLite doesn't enforce foreign keys (FKs). This event
    listeners emits the PRAGMA command to turn FK enforcement on. This
    should be attached as a SQLAlchemy listerer to the task database if and
    only if the database backend is sqlite.

    Futher details:

    https://docs.sqlalchemy.org/en/20/dialects/sqlite.html#foreign-key-support
    """
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
        max_tries: int
            the maximum number of trials for this task (this is total
            tries, so retries + 1)
        """
        raise NotImplementedError()

    @abc.abstractmethod
    def add_task_network(self, task_network: nx.DiGraph, max_tries: int):
        """Add a network of tasks to the database.

        Parameters
        ----------
        task_network: nx.Digraph
            A network with taskids (str) as nodes. Edges in this graph
            follow the direction of time/flow of information: from earlier
            tasks to later tasks; from requirements to subsequent.
        max_tries: int
            the maximum number of trials for these tasks (this is total
            tries, so retries + 1)
        """
        raise NotImplementedError()

    @abc.abstractmethod
    def check_out_task(self) -> Union[str, None]:
        """
        Select a task to be run.

        This should include any internal updates to the task status database
        to indicate that the given task has been checked out by a worker.

        Returns
        -------
        str | None :
            The taskid of the task to run; None if there are no available
            tasks
        """
        raise NotImplementedError()

    # we're probably going to want something like this in the future to
    # distinguish between failures in the execution system and failures
    # during a task
    # @abc.abstractmethod
    # def mark_task_aborted_incomplete(self, taskid: str):
    #     """
    #     Update the database when a task fails to complete.

    #     This may be caused by, e.g., a walltime limit being hit.

    #     Parameters
    #     ----------
    #     taskid: str
    #         the taskid of the failed task
    #     """
    #     raise NotImplementedError()

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
            and not sqla.event.contains(engine, "connect", _sqlite_fk_pragma)
        ):
            sqla.event.listen(engine, "connect", _sqlite_fk_pragma)

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

    def get_all_tasks(self) -> Iterable[sqla.Row]:
        """Yield current row for all tasks.

        This is mainly intended for debug and development usage; a more
        standardized variant will likely become part of the main API when we
        want dashboards, etc.
        """
        with self.engine.connect() as conn:
            yield from conn.execute(sqla.select(self.tasks_table)).all()

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
        sqla.event.listen(engine, "connect", _sqlite_fk_pragma)
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
            sqla.Column("blocking", sqla.Boolean),
        )
        # TODO: create indices that may be needed for performance
        metadata.create_all(bind=engine)

    @staticmethod
    def _get_task_and_dep_data(taskid: str, requirements: Iterable[str],
                               max_tries: int):
        """Create a dicts with database info based on a task to add.

        Parameters
        ----------
        taskid: str
            the taskid for this task
        requirements: Iterable[str]
            taskids of any tasks that are immediate blockers of this task
            (typically, this means that that outputs of tasks in this list
            are inputs to this task)
        max_tries: int
            the maximum number of trials for this task (this is total
            tries, so retries + 1)

        Returns
        -------
        task_data: Dict
            Dict with keys/value types as:

            * 'taskid': str
            * 'status': int
            * 'last_modified': datetime | None
            * 'tries': int
            * 'max_tries': int

        deps_data: List[Dict]
            list of dicts describing dependencies between tasks. Each dict
            has keys 'from', 'to', and 'blocking'. The values of 'from' and
            'to' are the taskid strings of tasks, where the 'from' task must
            complete before the 'to' task. The for 'blocking' is a boolean
            which indicates whether this dependency is still blocking
            (namely, whether the 'from' task has completed successfully).
        """
        stat = TaskStatus.BLOCKED if requirements else TaskStatus.AVAILABLE
        task_data = {
            'taskid': taskid,
            'status': stat.value,
            'last_modified': None,
            'tries': 0,
            'max_tries': max_tries
        }

        deps_data = [
            {'from': req, 'to': taskid, 'blocking': True}
            for req in requirements
        ]
        return [task_data], deps_data

    def _insert_task_and_deps_data(self, task_data, deps_data):
        """Insert data into database.

        This performs the actual insertion of task data into a database
        after it has been normalized to a form suitable for multiple tasks.

        The inputs dicts to this come from running
        :method:`._get_task_and_dep_data` on each task.

        Parameters
        ----------
        task_data: List[Dict]
            list of dicts describing tasks. Each dict consists of the
            following keys (with type after colon):

            * 'taskid': str
            * 'status': int
            * 'last_modified': datetime | None
            * 'tries': int
            * 'max_tries': int

        deps_data: List[Dict]
            list of dicts describing dependencies between tasks. Each dict
            has keys 'from', 'to', and 'blocking'. The values of 'from' and
            'to' are the taskid strings of tasks, where the 'from' task must
            complete before the 'to' task. The for 'blocking' is a boolean
            which indicates whether this dependency is still blocking
        """
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
        max_tries: int
            the maximum number of trials for this task (this is total
            tries, so retries + 1)
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
        max_tries: int
            the maximum number of trials for these tasks (this is total
            tries, so retries + 1)
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

    def _task_row_update_statement(
        self,
        taskid: str,
        status: Union[TaskStatus, SQLStatement],
        *,
        is_checkout: bool = False,
        max_tries: Optional[int] = None,
        old_status: Optional[TaskStatus] = None,
    ) -> SQLStatement:
        """
        Parameters
        ----------
        taskid: str
            task ID of the task to update
        status: TaskStatus
            the status to change to
        is_checkout: bool
            True if this is a checkout operation (and therefore should
            update the number of tries)
        max_tries: Optional[int]
            value to set for the maximum number of trials for this task
            (this is total tries, so retries + 1)
        old_status: Optional[TaskStatus]
            the previous status. If specified, the task will only match if
            this is its current status. Default (None) allows update from
            any previous status

        Returns
        -------
        SQLStatement :
            The statement to be executed to update this task row. This
            statement will return the rows that have been changed by the
            update.

        See also
        --------
        _validate_update_result : validate result from statement's execution
        """
        stmt = (
            sqla.update(self.tasks_table)
            .where(self.tasks_table.c.taskid == taskid)
        )

        if isinstance(status, TaskStatus):
            status = status.value

        if old_status is not None:
            stmt = stmt.where(self.tasks_table.c.status == old_status.value)

        # create a dict of values to update
        values = {
            'status': status,
            'last_modified': datetime.now(),
        }

        if is_checkout:
            values['tries'] = self.tasks_table.c.tries + 1

        if max_tries is not None:
            values['max_tries'] = max_tries

        stmt = stmt.values(**values)
        return stmt

    @staticmethod
    def _validate_update_result(result):
        """The result of an update statement should only have 1 changed row.

        This gives standard validation to raise errors if things went wrong.

        Raises
        ------
        RuntimeError
            Something went really wrong; you appear to have duplicate taskid
            rows. This should never occur.
        NoStatusChange
            The update couldn't happen. This likely means that the database
            allowed another process to mark the task as the updated status.
        """
        if result.rowcount > 1: # -no-cov-
            raise RuntimeError("Database seems to have more than 1 row with"
                               f"taskid '{taskid}'. This should not happen.")
        elif result.rowcount == 0:
            raise NoStatusChange(f"Task '{taskid}' could not change from "
                                 f"{old_status} to {status}")

    def check_out_task(self):
        # TODO: may need move this to a single attempt function and wrap it
        # in while loop to catch NoStatusChange errors until we have a
        # successful checkout

        # TODO: separate selection so subclasses can easily override;
        # something like `_select_task(conn: Connection) -> Row` (allow us
        # to do something smarter than "take the first available")
        sel_stmt = (
            sqla.select(self.tasks_table)
            .where(self.tasks_table.c.status == TaskStatus.AVAILABLE.value)
        )
        with self.engine.begin() as conn:
            task_row = conn.execute(sel_stmt).first()

            if task_row is None:
                # no tasks are available
                return None

            update_stmt = self._task_row_update_statement(
                task_row.taskid,
                status=TaskStatus.IN_PROGRESS,
                is_checkout=True
            )
            result = conn.execute(update_stmt)

            self._validate_update_result(result)

        return task_row.taskid

    def _mark_task_completed_failure(self, taskid: str):
        status_statement = sqla.case(
            (
                self.tasks_table.c.tries >= self.tasks_table.c.max_tries,
                TaskStatus.TOO_MANY_RETRIES.value
            ),
            else_=TaskStatus.AVAILABLE.value
        )
        update_task_finished_fail = self._task_row_update_statement(
            taskid,
            status=status_statement,
            old_status=TaskStatus.IN_PROGRESS
        )
        with self.engine.begin() as conn:
            result = conn.execute(update_task_finished_fail)
            self._validate_update_result(result)


    def _mark_task_completed_success(self, taskid: str):
        _logger.debug(f"Marking task '{taskid}' as successfully completed")
        # TODO: there may be ways to make this faster; this is likely to be
        # the most important point for performance considerations

        # as a minor performance point, we create the SQL statements before
        # executing them in the transaction

        # conveniences (esp since from is a reserved word)
        from_col = getattr(self.dependencies_table.c, "from")
        to_col = self.dependencies_table.c.to

        # 1. UPDATE the task status as completed
        update_task_completed = self._task_row_update_statement(
            taskid,
            status=TaskStatus.COMPLETED,
            old_status=TaskStatus.IN_PROGRESS
        )

        # 2. UPDATE all dependency rows where from==taskid to mark these as
        #    no longer blocking; RETURNING 'to'
        update_deps = (
            sqla.update(self.dependencies_table)
            .where(from_col == taskid)
            .values(blocking=False)
            .returning(to_col)
        )

        # 3. SELECT the tasks among the tasks that have been partially
        #    unblocked that are actually still blocked. For now, we'll do a
        #    set difference in Python.
        #    NOTE: this is the tricky bit, where several implementations
        #    might be worth trying for performance.
        still_blocked = (
            sqla.select(to_col)
            .filter(to_col.in_(sqla.bindparam("candidates")))
            .where(self.dependencies_table.c.blocking == True)
        )

        # 4. UPDATE tasks table to mark these tasks as available
        update_task_unblocked = self._task_row_update_statement(
            taskid=sqla.bindparam('unblock'),
            status=TaskStatus.AVAILABLE,
            old_status=TaskStatus.BLOCKED
        )

        # now we actually DO those steps
        with self.engine.begin() as conn:
            _logger.debug("Setting task status to COMPLETED")
            completed_task = conn.execute(update_task_completed)
            _logger.debug("Identifying candidates to unblock")
            candidates = conn.execute(update_deps).fetchall()
            candidates = {c[0] for c in candidates}
            _logger.debug("Identifying which candidates should unblocked")
            blocked = conn.execute(
                still_blocked, {"candidates": candidates}
            ).fetchall()
            blocked = {c[0] for c in blocked}
            to_unblock = candidates - blocked
            if to_unblock:
                _logger.debug("Moving unblocked tasks to AVAILABLE")
                unblocked = conn.execute(update_task_unblocked, [
                    {'unblock': unblock} for unblock in to_unblock
                ])
            else:
                _logger.debug("No tasks to unblock")

    def mark_task_completed(self, taskid: str, success: bool):
        if success:
            return self._mark_task_completed_success(taskid)
        else:
            return self._mark_task_completed_failure(taskid)

    # TODO: add a method that forces consistency between task table's status
    # and the dependencies table (no completed task should be blocking
    # anything)
    # def update_dependencies_to_match_tasks(self):
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
