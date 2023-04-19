import pytest
from exorcist import TaskStatusDB, NoStatusChange, TaskStatus

import sqlalchemy as sqla

def create_database(metadata, engine, extra_table=False,
                    missing_table=False, extra_column=False,
                    missing_column=False, bad_types=False):
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
    ]
    deps_columns = [
        sqla.Column("from", sqla.String, sqla.ForeignKey("tasks.taskid")),
        sqla.Column("to", sqla.String, sqla.ForeignKey("tasks.taskid")),
    ]

    if missing_column:
        task_columns = task_columns[:-1]
    if extra_column:
        task_columns.append(sqla.Column("foo", sqla.String))

    tasks_table = sqla.Table("tasks", metadata, *task_columns)
    if not missing_table:
        deps_talbe = sqla.Table("dependencies", metadata, *deps_columns)

    if extra_table:
        extra_table = sqla.Table("bar", metadata,
                                 sqla.Column("baz", sqla.String))

    metadata.create_all(bind=engine)

def add_mock_data(metadata, engine):
    # add a couple of tasks for when we want to pretend we're opening an
    # existing file
    tasks = [
        {'taskid': "foo", "status": TaskStatus.AVAILABLE.value,
         'last_modified': None, 'tries': 0},
        {'taskid': "bar", "status": TaskStatus.BLOCKED.value,
         'last_modified': None, 'tries': 0}
    ]
    deps = [{'from': "foo", 'to': "bar"}]

    ins_tasks = sqla.insert(metadata.tables['tasks']).values(tasks)
    ins_deps = sqla.insert(metadata.tables['dependencies']).values(deps)

    with engine.connect() as conn:
        res1 = conn.execute(ins_tasks)
        res2 = conn.execute(ins_deps)
        conn.commit()


@pytest.fixture
def fresh_db():
    return TaskStatusDB(sqla.create_engine("sqlite://"))

@pytest.fixture
def loaded_db(fresh_db):
    add_mock_data(fresh_db.metadata, fresh_db.engine)
    return fresh_db


class TestTaskStatusDB:
    @staticmethod
    def assert_is_our_db(db):
        assert len(db.metadata.tables) == 2
        assert set(db.metadata.tables) == {'tasks', 'dependencies'}

    @staticmethod
    def assert_is_fresh_db(db):
        TestTaskStatusDB.assert_is_our_db(db)

        def count_rows(table):
            query = sqla.select(sqla.func.count()).select_from(table)
            with db.engine.connect() as conn:
                count = conn.execute(query).scalar()
            return count

        assert count_rows(db.metadata.tables['tasks']) == 0
        assert count_rows(db.metadata.tables['dependencies']) == 0

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
            assert deps == {("foo", "bar")}
            assert tasks == {
                ('foo', TaskStatus.AVAILABLE.value, None, 0),
                ('bar', TaskStatus.BLOCKED.value, None, 0),
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
        create_database(engine, metadata, **kwargs)
        with pytest.raises(RuntimeError, "not seem to be a task database"):
            TaskStatusDB(engine)


    @pytest.mark.parametrize('fixture', ['fresh_db', 'loaded_db'])
    def test_tasks_table(self, request, fixture):
        expected = {
            'fresh_db': set(),
            'loaded_db': {('foo', TaskStatus.AVAILABLE.value, None, 0),
                          ('bar', TaskStatus.BLOCKED.value, None, 0)},
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
            'loaded_db': {("foo", "bar")},
        }[fixture]
        db = request.getfixturevalue(fixture)
        with db.engine.connect() as conn:
            deps = list(conn.execute(sqla.select(db.dependencies_table)))

        assert len(deps) == len(expected)
        assert set(deps) == expected
