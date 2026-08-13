import pytest
from unittest import mock
from exorcist import TaskStatusDB, NoStatusChange, TaskStatus
from exorcist.taskdb import _logger as taskdb_logger

import os
import re
import sqlalchemy as sqla
import networkx as nx
from datetime import datetime
import logging

def create_database(metadata, engine, extra_table=False,
                    missing_table=False, extra_column=False,
                    missing_column=False, bad_types=False,
                    include_task_type=True):
    """
    Create databases with various imperfections on our schema.
    """
    # use the status column to check for bad types
    status_type = sqla.String if bad_types else sqla.Integer
    task_columns = [
        sqla.Column("taskid", sqla.String, primary_key=True),
        sqla.Column("status", status_type),
        sqla.Column("last_modified", sqla.DateTime),
        sqla.Column("tries", sqla.Integer),
        sqla.Column("max_tries", sqla.Integer),
    ]
    deps_columns = [
        sqla.Column("from", sqla.String, sqla.ForeignKey("tasks.taskid")),
        sqla.Column("to", sqla.String, sqla.ForeignKey("tasks.taskid")),
        sqla.Column("blocking", sqla.Boolean),
    ]

    if missing_column:
        task_columns = [
            column for column in task_columns if column.name != "max_tries"
        ]
    if extra_column:
        task_columns.append(sqla.Column("foo", sqla.String))

    tasks_table = sqla.Table("tasks", metadata, *task_columns)
    if not missing_table:
        deps_talbe = sqla.Table("dependencies", metadata, *deps_columns)
    if include_task_type:
        types_table = sqla.Table(
            "task_types",
            metadata,
            sqla.Column("taskid", sqla.String,
                        sqla.ForeignKey("tasks.taskid"), primary_key=True),
            sqla.Column("type", sqla.String),
        )

    if extra_table:
        extra_table = sqla.Table("bar", metadata,
                                 sqla.Column("baz", sqla.String))

    metadata.create_all(bind=engine)

def task_row(taskid, status, last_modified, tries, max_tries):
    return (taskid, status.value, last_modified, tries, max_tries)

def add_mock_data(metadata, engine, tries=0, status=TaskStatus.AVAILABLE,
                  task_type=""):
    # add a couple of tasks for when we want to pretend we're opening an
    # existing file
    tasks = [
        {'taskid': "foo", "status": status.value,
         'last_modified': None, 'tries': tries, 'max_tries': 3},
        {'taskid': "bar", "status": TaskStatus.BLOCKED.value,
         'last_modified': None, 'tries': 0, 'max_tries': 3}
    ]
    deps = [{'from': "foo", 'to': "bar", 'blocking': True}]

    ins_tasks = sqla.insert(metadata.tables['tasks']).values(tasks)
    ins_deps = sqla.insert(metadata.tables['dependencies']).values(deps)

    with engine.connect() as conn:
        res1 = conn.execute(ins_tasks)
        res2 = conn.execute(ins_deps)
        if task_type:
            conn.execute(sqla.insert(metadata.tables['task_types']).values(
                taskid="foo", type=task_type
            ))
        conn.commit()

_DEFAULT_DATETIME = datetime(1970, 1, 1)

def patch_datetime_now(with_datetime=_DEFAULT_DATETIME):
    # turns out we can't patch just the now() method (datetime is immutable,
    # probably C code?) so we have to patch the entire datetime module
    loc = "exorcist.taskdb.datetime"
    datetime_now = mock.Mock(now=mock.Mock(return_value=with_datetime))
    return mock.patch(loc, datetime_now)

@pytest.fixture
def db_connect_string():
    # defines the connection string for our test database; this is used by
    # the fresh_db fixture, and is determined at runtime by the
    # EXORCIST_TEST_DB environment variable (defaults to sqlite). Use this
    # to change which database backend we test against.
    db_type = os.environ.get("EXORCIST_TEST_DB", "sqlite")
    connect_string = {
        "sqlite": "sqlite://",
        "postgres": "postgresql+psycopg://127.0.0.1:5432/exorcist_testdb",
    }[db_type]
    # print(f"USING {connect_string}")  # DEBUG
    return connect_string

@pytest.fixture
def fresh_db(db_connect_string):
    # create an empty database; for "real" (non-sqlite) databases, drop
    # existing tables
    echo = False  # switch this for debugging
    engine = sqla.create_engine(db_connect_string, echo=echo)

    if engine.name == "postgresql":
        # force empty with DROP CASCADE
        bobby = """
        DROP TABLE IF EXISTS
          tasks,
          dependencies,
          task_types
        CASCADE
        """
        with engine.begin() as conn:
            conn.execute(sqla.text(bobby))

    yield TaskStatusDB(engine)

@pytest.fixture
def loaded_db(fresh_db):
    # database with simple dependencies (foo blocks bar)
    add_mock_data(fresh_db.metadata, fresh_db.engine)
    return fresh_db

@pytest.fixture
def vshape_db(fresh_db):
    # data for testing a "V" shape of dependencies (foo and bar both block
    # baz)
    metadata = fresh_db.metadata
    engine = fresh_db.engine
    tasks = [
        {'taskid': "foo", "status": TaskStatus.AVAILABLE.value,
         'last_modified': None, 'tries': 0, 'max_tries': 3},
        {'taskid': "bar", "status": TaskStatus.AVAILABLE.value,
         'last_modified': None, 'tries': 0, 'max_tries': 3},
        {'taskid': "baz", "status": TaskStatus.BLOCKED.value,
         'last_modified': None, 'tries': 0, 'max_tries': 3},
    ]
    deps = [{'from': "foo", 'to': "baz", 'blocking': True},
            {'from': "bar", 'to': "baz", 'blocking': True}]

    ins_tasks = sqla.insert(metadata.tables['tasks']).values(tasks)
    ins_deps = sqla.insert(metadata.tables['dependencies']).values(deps)

    with engine.connect() as conn:
        res1 = conn.execute(ins_tasks)
        res2 = conn.execute(ins_deps)
        conn.commit()

    return fresh_db

def count_rows(db, table):
    query = sqla.select(sqla.func.count()).select_from(table)
    with db.engine.connect() as conn:
        count = conn.execute(query).scalar()
    return count

def get_tasks_and_deps(db):
    with db.engine.connect() as conn:
        tasks = set(conn.execute(sqla.select(db.tasks_table)))
        deps = set(conn.execute(sqla.select(db.dependencies_table)))
    return tasks, deps

def get_task_types(db):
    with db.engine.connect() as conn:
        return set(conn.execute(sqla.select(db.task_types_table)))

@pytest.fixture
def diamond_taskid_network():
    graph = nx.DiGraph()
    graph.add_nodes_from(["A", "B", "C", "D"])
    graph.add_edges_from([("A", "B"), ("A", "C"), ("B", "D"), ("C", "D")])
    return graph

class TestTaskStatusDB:
    @staticmethod
    def assert_is_our_db(db):
        assert len(db.metadata.tables) == 3
        assert set(db.metadata.tables) == {
            'tasks', 'dependencies', 'task_types'
        }
        assert set(db.task_types_table.c.keys()) == {"taskid", "type"}

    @staticmethod
    def assert_is_fresh_db(db):
        TestTaskStatusDB.assert_is_our_db(db)
        assert count_rows(db, db.metadata.tables['tasks']) == 0
        assert count_rows(db, db.metadata.tables['dependencies']) == 0
        assert count_rows(db, db.metadata.tables['task_types']) == 0

    def test_fresh_db(self, fresh_db):
        # this effectively tests that the __init__ method is working
        self.assert_is_fresh_db(fresh_db)

    @pytest.mark.parametrize('existing', [True, False])
    @pytest.mark.parametrize('overwrite', [True, False])
    def test_from_filename(self, overwrite, existing, tmpdir):
        filename = tmpdir / "foo.db"
        if existing:
            engine = sqla.create_engine(f"sqlite:///{filename}")
            metadata = sqla.MetaData()
            create_database(metadata, engine)
            add_mock_data(metadata, engine)

        db = TaskStatusDB.from_filename(filename, overwrite=overwrite)

        if overwrite or not existing:
            self.assert_is_fresh_db(db)
        else:
            # if we don't overwrite and it does exist, make sure we still
            # have our content
            self.assert_is_our_db(db)
            with db.engine.connect() as conn:
                tasks = set(conn.execute(sqla.select(db.tasks_table)))
                deps = set(conn.execute(sqla.select(db.dependencies_table)))

            assert len(tasks) == 2
            assert len(deps) == 1
            assert deps == {("foo", "bar", True)}
            assert tasks == {
                task_row('foo', TaskStatus.AVAILABLE, None, 0, 3),
                task_row('bar', TaskStatus.BLOCKED, None, 0, 3),
            }

    # leaving this xfail for now because implementing the functionality
    # isn't a high priority
    @pytest.mark.xfail
    @pytest.mark.parametrize('fail_reason', [
        'missing_table', 'extra_table', 'missing_column', 'extra_column',
        'bad_types'
    ])
    def test_error_db_is_not_ours(self, fail_reason):
        engine = sqla.create_engine("sqlite://")
        metadata = sqla.MetaData()
        kwargs = {fail_reason: True}
        create_database(metadata, engine, **kwargs)
        with pytest.raises(RuntimeError, "not seem to be a task database"):
            TaskStatusDB(engine)

    @pytest.mark.parametrize('fixture', ['fresh_db', 'loaded_db'])
    def test_tasks_table(self, request, fixture):
        expected = {
            'fresh_db': set(),
            'loaded_db': {task_row('foo', TaskStatus.AVAILABLE, None, 0, 3),
                          task_row('bar', TaskStatus.BLOCKED, None, 0, 3)},
        }[fixture]
        db = request.getfixturevalue(fixture)
        with db.engine.connect() as conn:
            tasks = list(conn.execute(sqla.select(db.tasks_table)))

        assert len(tasks) == len(expected)
        assert set(tasks) == expected

    @pytest.mark.parametrize('fixture', ['fresh_db', 'loaded_db'])
    def test_dependencies_table(self, request, fixture):
        expected = {
            'fresh_db': set(),
            'loaded_db': {("foo", "bar", True)},
        }[fixture]
        db = request.getfixturevalue(fixture)
        with db.engine.connect() as conn:
            deps = list(conn.execute(sqla.select(db.dependencies_table)))

        assert len(deps) == len(expected)
        assert set(deps) == expected

    @pytest.mark.parametrize('fixture', ['fresh_db', 'loaded_db'])
    def test_get_all_tasks(self, request, fixture):
        expected = {
            'fresh_db': set(),
            'loaded_db': {task_row('foo', TaskStatus.AVAILABLE, None, 0, 3),
                task_row('bar', TaskStatus.BLOCKED, None, 0, 3)},
        }[fixture]
        db = request.getfixturevalue(fixture)
        tasks = set(db.get_all_tasks())
        assert tasks == expected

    def test_add_task(self, fresh_db):
        # task without prerequisites
        fresh_db.add_task("foo", requirements=[], max_tries=3)
        expected_foo_task = task_row("foo", TaskStatus.AVAILABLE, None, 0, 3)
        tasks, deps = get_tasks_and_deps(fresh_db)
        assert set(tasks) == {expected_foo_task}
        assert set(deps) == set()

        # task with prerequisites
        fresh_db.add_task("bar", requirements=['foo'], max_tries=3)
        tasks, deps = get_tasks_and_deps(fresh_db)
        assert set(tasks) == {expected_foo_task,
                              task_row("bar", TaskStatus.BLOCKED, None, 0, 3)}
        assert set(deps) == {("foo", "bar", True)}

    def test_add_task_with_task_type(self, fresh_db):
        fresh_db.add_task("gpu-task", requirements=[], max_tries=3,
                          task_type="gpu")
        tasks, deps = get_tasks_and_deps(fresh_db)
        assert tasks == {task_row("gpu-task", TaskStatus.AVAILABLE, None,
                                  0, 3)}
        assert deps == set()
        assert get_task_types(fresh_db) == {("gpu-task", "gpu")}

    def test_add_task_before_requirements(self, fresh_db):
        fk_regex = re.compile("foreign key", re.I)
        with pytest.raises(sqla.exc.IntegrityError, match=fk_regex):
            fresh_db.add_task("bar", requirements=["foo"], max_tries=3)

        # check that task insertion got rolled back
        self.assert_is_fresh_db(fresh_db)

    def test_add_task_network(self, fresh_db, diamond_taskid_network):
        fresh_db.add_task_network(diamond_taskid_network, max_tries=3)
        tasks, deps = get_tasks_and_deps(fresh_db)
        expected_tasks = {
            task_row("A", TaskStatus.AVAILABLE, None, 0, 3),
            task_row("B", TaskStatus.BLOCKED, None, 0, 3),
            task_row("C", TaskStatus.BLOCKED, None, 0, 3),
            task_row("D", TaskStatus.BLOCKED, None, 0, 3),
        }
        expected_deps = {("A", "B", True), ("A", "C", True),
                         ("B", "D", True), ("C", "D", True)}
        assert set(tasks) == expected_tasks
        assert set(deps) == expected_deps
        assert len(tasks) == len(expected_tasks)
        assert len(deps) == len(expected_deps)

    def test_add_task_network_with_task_types(self, fresh_db,
                                              diamond_taskid_network):
        fresh_db.add_task_network(
            diamond_taskid_network,
            max_tries=3,
            task_types={"A": "cpu", "D": "gpu"},
        )
        tasks, _ = get_tasks_and_deps(fresh_db)
        assert tasks == {
            task_row("A", TaskStatus.AVAILABLE, None, 0, 3),
            task_row("B", TaskStatus.BLOCKED, None, 0, 3),
            task_row("C", TaskStatus.BLOCKED, None, 0, 3),
            task_row("D", TaskStatus.BLOCKED, None, 0, 3),
        }
        assert get_task_types(fresh_db) == {("A", "cpu"), ("D", "gpu")}

    def test_add_task_network_unknown_task_type_key(self, fresh_db,
                                                    diamond_taskid_network):
        with pytest.raises(ValueError, match=r"unknown tasks: \[\'missing\'\]"):
            fresh_db.add_task_network(
                diamond_taskid_network,
                max_tries=3,
                task_types={"missing": "gpu"},
            )

    def test_task_row_update_statement(self, loaded_db):
        # TODO: I'm going to do this in a future PR
        ...

    # be sure to cover extra code paths when doing DEBUG logging
    @pytest.mark.parametrize('loglevel', [logging.DEBUG, logging.WARNING])
    def test_check_out_task(self, loaded_db, loglevel):
        taskdb_logger.setLevel(loglevel)

        taskid = loaded_db.check_out_task()
        assert taskid == "foo"

        tasks, deps = get_tasks_and_deps(loaded_db)
        taskdict = {t[0]: t for t in tasks}
        foo = taskdict["foo"]
        bar = taskdict["bar"]
        assert foo.taskid == "foo"
        assert foo.status == TaskStatus.IN_PROGRESS.value
        assert foo.tries == 1
        assert foo.max_tries == 3

        assert bar == task_row("bar", TaskStatus.BLOCKED, None, 0, 3)
        assert get_task_types(loaded_db) == set()
        taskdb_logger.setLevel(logging.NOTSET)

    def test_task_type_preserved_through_checkout_and_completion(self,
                                                                 fresh_db):
        fresh_db.add_task("gpu-task", requirements=[], max_tries=3,
                          task_type="gpu")

        with patch_datetime_now():
            taskid = fresh_db.check_out_task()
        assert taskid == "gpu-task"

        with patch_datetime_now():
            fresh_db.mark_task_completed(taskid, success=True)

        tasks, deps = get_tasks_and_deps(fresh_db)
        assert tasks == {task_row("gpu-task", TaskStatus.COMPLETED,
                                  _DEFAULT_DATETIME, 1, 3)}
        assert deps == set()
        assert get_task_types(fresh_db) == {("gpu-task", "gpu")}

    def test_check_out_task_double_checkout(self, loaded_db):
        taskid = loaded_db.check_out_task()
        assert taskid == "foo"
        # now there should be no available tasks, so a checkout here will
        # return None
        assert loaded_db.check_out_task() is None

    def test_check_out_task_repeated_checkout_vshape(self, vshape_db):
        sel1 = vshape_db.check_out_task()
        sel2 = vshape_db.check_out_task()
        sel3 = vshape_db.check_out_task()
        # order isn't guaranteed here, so use a set
        assert {sel1, sel2} == {"foo", "bar"}
        assert sel3 is None

    def test_check_out_task_empty_db(self, fresh_db):
        assert fresh_db.check_out_task() is None

    def test_check_out_task_no_available(self, fresh_db):
        add_mock_data(fresh_db.metadata, fresh_db.engine, tries=1,
                      status=TaskStatus.IN_PROGRESS)
        assert fresh_db.check_out_task() is None

    def test_mark_task_completed_failure_retry(self, fresh_db):
        add_mock_data(fresh_db.metadata, fresh_db.engine, tries=1,
                      status=TaskStatus.IN_PROGRESS)
        # assert that our initial conditions are as expected
        tasks, deps = get_tasks_and_deps(fresh_db)
        assert tasks == {
            task_row("foo", TaskStatus.IN_PROGRESS, None, 1, 3),
            task_row("bar", TaskStatus.BLOCKED, None, 0, 3),
        }
        assert deps == {("foo", "bar", True)}

        with patch_datetime_now():
            fresh_db.mark_task_completed("foo", success=False)

        tasks, deps = get_tasks_and_deps(fresh_db)
        assert tasks == {
            task_row("foo", TaskStatus.AVAILABLE, _DEFAULT_DATETIME, 1, 3),
            task_row("bar", TaskStatus.BLOCKED, None, 0, 3),
        }
        assert deps == {("foo", "bar", True)}

    def test_mark_task_completed_failure_max_tries(self, fresh_db):
        add_mock_data(fresh_db.metadata, fresh_db.engine, tries=3,
                      status=TaskStatus.IN_PROGRESS)
        # assert that our initial conditions are as expected
        tasks, deps = get_tasks_and_deps(fresh_db)
        assert tasks == {
            task_row("foo", TaskStatus.IN_PROGRESS, None, 3, 3),
            task_row("bar", TaskStatus.BLOCKED, None, 0, 3),
        }
        assert deps == {("foo", "bar", True)}

        with patch_datetime_now():
            fresh_db.mark_task_completed("foo", success=False)

        tasks, deps = get_tasks_and_deps(fresh_db)
        assert tasks == {
            task_row("foo", TaskStatus.TOO_MANY_RETRIES, _DEFAULT_DATETIME,
                     3, 3),
            task_row("bar", TaskStatus.BLOCKED, None, 0, 3),
        }
        assert deps == {("foo", "bar", True)}

    def test_mark_task_completed_failure_more_than_max_tries(self, fresh_db):
        # In practice, this shouldn't occur during normal operation, but
        # might occur in the future if a user reduces max_tries while
        # execution is in progress. In that special case, the code will
        # still act the same on success; this checks that on failure, we no
        # longer retry.
        add_mock_data(fresh_db.metadata, fresh_db.engine, tries=5,
                      status=TaskStatus.IN_PROGRESS)
        with patch_datetime_now():
            fresh_db.mark_task_completed("foo", success=False)

        tasks, deps = get_tasks_and_deps(fresh_db)
        assert tasks == {
            task_row("foo", TaskStatus.TOO_MANY_RETRIES, _DEFAULT_DATETIME,
                     5, 3),
            task_row("bar", TaskStatus.BLOCKED, None, 0, 3),
        }
        assert deps == {("foo", "bar", True)}

    def test_mark_task_completed_success(self, fresh_db):
        add_mock_data(fresh_db.metadata, fresh_db.engine,
                      status=TaskStatus.IN_PROGRESS, tries=1)
        tasks, deps = get_tasks_and_deps(fresh_db)
        assert tasks == {
            task_row("foo", TaskStatus.IN_PROGRESS, None, 1, 3),
            task_row("bar", TaskStatus.BLOCKED, None, 0, 3),
        }
        assert deps == {("foo", "bar", True)}

        with patch_datetime_now():
            fresh_db.mark_task_completed("foo", success=True)

        tasks, deps = get_tasks_and_deps(fresh_db)
        assert tasks == {
            task_row("foo", TaskStatus.COMPLETED, _DEFAULT_DATETIME, 1, 3),
            task_row("bar", TaskStatus.AVAILABLE, _DEFAULT_DATETIME, 0, 3),
        }
        assert deps == {("foo", "bar", False)}

    def test_diamond_network_sequence(self, fresh_db,
                                      diamond_taskid_network):
        # this does the whole process of adding a task network to a database
        # and running it step by step.
        fresh_db.add_task_network(diamond_taskid_network, max_tries=3)
        tasks, deps = get_tasks_and_deps(fresh_db)
        assert tasks == {
            task_row("A", TaskStatus.AVAILABLE, None, 0, 3),
            task_row("B", TaskStatus.BLOCKED, None, 0, 3),
            task_row("C", TaskStatus.BLOCKED, None, 0, 3),
            task_row("D", TaskStatus.BLOCKED, None, 0, 3),
        }
        assert deps == {("A", "B", True), ("A", "C", True),
                        ("B", "D", True), ("C", "D", True)}

        # check out first task
        datetime_sel1 = datetime(1970, 1, 1)
        with patch_datetime_now(datetime_sel1):
            sel1 = fresh_db.check_out_task()
        assert sel1 == "A"
        tasks, deps = get_tasks_and_deps(fresh_db)
        assert tasks == {
            task_row("A", TaskStatus.IN_PROGRESS, datetime_sel1, 1, 3),
            task_row("B", TaskStatus.BLOCKED, None, 0, 3),
            task_row("C", TaskStatus.BLOCKED, None, 0, 3),
            task_row("D", TaskStatus.BLOCKED, None, 0, 3),
        }
        assert deps == {("A", "B", True), ("A", "C", True),
                        ("B", "D", True), ("C", "D", True)}

        # finish first task
        datetime_fin1 = datetime(1970, 1, 2)
        with patch_datetime_now(datetime_fin1):
            fin1 = fresh_db.mark_task_completed(sel1, success=True)

        tasks, deps = get_tasks_and_deps(fresh_db)
        assert tasks == {
            task_row("A", TaskStatus.COMPLETED, datetime_fin1, 1, 3),
            task_row("B", TaskStatus.AVAILABLE, datetime_fin1, 0, 3),
            task_row("C", TaskStatus.AVAILABLE, datetime_fin1, 0, 3),
            task_row("D", TaskStatus.BLOCKED, None, 0, 3),
        }
        assert deps == {("A", "B", False), ("A", "C", False),
                        ("B", "D", True), ("C", "D", True)}

        # check out second task
        datetime_sel2 = datetime(1970, 2, 1)
        with patch_datetime_now(datetime_sel2):
            sel2 = fresh_db.check_out_task()

        sel3 = {"B": "C", "C": "B"}[sel2]  # KeyError means bad sel2 value
        tasks, deps = get_tasks_and_deps(fresh_db)
        assert tasks == {
            task_row("A", TaskStatus.COMPLETED, datetime_fin1, 1, 3),
            task_row(sel2, TaskStatus.IN_PROGRESS, datetime_sel2, 1, 3),
            task_row(sel3, TaskStatus.AVAILABLE, datetime_fin1, 0, 3),
            task_row("D", TaskStatus.BLOCKED, None, 0, 3),
        }
        assert deps == {("A", "B", False), ("A", "C", False),
                        ("B", "D", True), ("C", "D", True)}

        # finish second task
        datetime_fin2 = datetime(1970, 2, 2)
        with patch_datetime_now(datetime_fin2):
            fin2 = fresh_db.mark_task_completed(sel2, success=True)

        tasks, deps = get_tasks_and_deps(fresh_db)
        assert tasks == {
            task_row("A", TaskStatus.COMPLETED, datetime_fin1, 1, 3),
            task_row(sel2, TaskStatus.COMPLETED, datetime_fin2, 1, 3),
            task_row(sel3, TaskStatus.AVAILABLE, datetime_fin1, 0, 3),
            task_row("D", TaskStatus.BLOCKED, None, 0, 3),
        }
        assert deps == {("A", "B", False), ("A", "C", False),
                        (sel2, "D", False), (sel3, "D", True)}

        # check out third task
        datetime_sel3 = datetime(1970, 3, 1)
        with patch_datetime_now(datetime_sel3):
            # assertion error here if sel2, sel3 not in
            # {(B, C), (C, B)} -- this is where we check that sel2 and sel3
            # were B and C
            assert fresh_db.check_out_task() == sel3

        tasks, deps = get_tasks_and_deps(fresh_db)
        assert tasks == {
            task_row("A", TaskStatus.COMPLETED, datetime_fin1, 1, 3),
            task_row(sel2, TaskStatus.COMPLETED, datetime_fin2, 1, 3),
            task_row(sel3, TaskStatus.IN_PROGRESS, datetime_sel3, 1, 3),
            task_row("D", TaskStatus.BLOCKED, None, 0, 3),
        }
        assert deps == {("A", "B", False), ("A", "C", False),
                        (sel2, "D", False), (sel3, "D", True)}

        # finish third task
        datetime_fin3 = datetime(1970, 3, 2)
        with patch_datetime_now(datetime_fin3):
            fin3 = fresh_db.mark_task_completed(sel3, success=True)

        tasks, deps = get_tasks_and_deps(fresh_db)
        assert tasks == {
            task_row("A", TaskStatus.COMPLETED, datetime_fin1, 1, 3),
            task_row(sel2, TaskStatus.COMPLETED, datetime_fin2, 1, 3),
            task_row(sel3, TaskStatus.COMPLETED, datetime_fin3, 1, 3),
            task_row("D", TaskStatus.AVAILABLE, datetime_fin3, 0, 3),
        }
        assert deps == {("A", "B", False), ("A", "C", False),
                        (sel2, "D", False), (sel3, "D", False)}

        # check out fourth task
        datetime_sel4 = datetime(1970, 4, 1)
        with patch_datetime_now(datetime_sel4):
            sel4 = fresh_db.check_out_task()

        assert sel4 == "D"
        tasks, deps = get_tasks_and_deps(fresh_db)
        assert tasks == {
            task_row("A", TaskStatus.COMPLETED, datetime_fin1, 1, 3),
            task_row(sel2, TaskStatus.COMPLETED, datetime_fin2, 1, 3),
            task_row(sel3, TaskStatus.COMPLETED, datetime_fin3, 1, 3),
            task_row("D", TaskStatus.IN_PROGRESS, datetime_sel4, 1, 3),
        }
        assert deps == {("A", "B", False), ("A", "C", False),
                        ("B", "D", False), ("C", "D", False)}

        # finish fourth task
        datetime_fin4 = datetime(1970, 4, 2)
        with patch_datetime_now(datetime_fin4):
            fin4 = fresh_db.mark_task_completed(sel4, success=True)

        tasks, deps = get_tasks_and_deps(fresh_db)
        assert tasks == {
            task_row("A", TaskStatus.COMPLETED, datetime_fin1, 1, 3),
            task_row(sel2, TaskStatus.COMPLETED, datetime_fin2, 1, 3),
            task_row(sel3, TaskStatus.COMPLETED, datetime_fin3, 1, 3),
            task_row("D", TaskStatus.COMPLETED, datetime_fin4, 1, 3),
        }
        assert deps == {("A", "B", False), ("A", "C", False),
                        ("B", "D", False), ("C", "D", False)}

        # if you check out another task, you get None (and DB is unchanged)
        sel5 = fresh_db.check_out_task()
        assert sel5 is None
        tasks, deps = get_tasks_and_deps(fresh_db)
        assert tasks == {
            task_row("A", TaskStatus.COMPLETED, datetime_fin1, 1, 3),
            task_row(sel2, TaskStatus.COMPLETED, datetime_fin2, 1, 3),
            task_row(sel3, TaskStatus.COMPLETED, datetime_fin3, 1, 3),
            task_row("D", TaskStatus.COMPLETED, datetime_fin4, 1, 3),
        }
        assert deps == {("A", "B", False), ("A", "C", False),
                        ("B", "D", False), ("C", "D", False)}
