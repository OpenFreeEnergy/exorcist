from datetime import datetime
from models import TaskStatus
import sqlalchemy as sqla

NRETRIES = 10   # maybe put this in an rcfile someday?

class UnavailableTaskException(Exception):
    ...


def pick_first(tasklist):
    return tasklist[0]


def _load_available_tasks(db_engine, tasks_table):
    select_available = sqla.select(tasks).where(
        tasks.c.status == TaskStatus.AVAILABLE
    )
    with db_engine.connect() as conn:
        curs = conn.execute(select_available)
        tasklist = list(curs.fetch_all())

    return tasklist


def _claim_task(db_engine, tasks_table, task):
    update = sqla.update(tasks_table).where(sqla.and_(
        tasks_table.c.id == task.id,
        tasks_table.c.status == TaskStatus.AVAILABLE
    )).values(
        status=TaskStatus.IN_PROGRESS,
        last_modified=datetime.utcnow()
    )
    with db_engine.connect() as conn:
        curs = conn.execute(update)
        conn.commit()

    if curs.rowcount == 0:
        return task
    elif curs.rowcount > 1:  # -no-cov-
        raise RuntimeError("More than 1 row affected by this update; "
                           "something has gone very wrong! "
                           f"\n{update}\nwith task id {task.id}")
    else:
        raise UnavailableTaskException()


def claim_task_attempt(
    db_engine,
    tasks_table,
    select_task=pick_first
):
    # select a task from the table
    tasklist = _load_available_tasks(db_engine, tasks_table)

    # early return if there are no tasks
    if not tasklist:
        return None

    task = select_task(tasklist)

    # update the tasks table to include status that this one is taken
    return _claim_task(db_engine, tasks_table, task)


def claim_task(db_engine, tasks_table, select_task):
    retries = 0
    task = None
    while task is None:
        try:
            task = claim_task_attempt(db_engine, tasks_table, select_task)
        except UnavailableTaskException:
            if retries == NRETRIES:
                raise

            n_retries += 1
            continue

    return task
