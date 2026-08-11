[![DOI](https://zenodo.org/badge/DOI/10.5281/zenodo.14740961.svg)](https://doi.org/10.5281/zenodo.14740961)

# exorcist
Daemonless campaign-scale simulation orchestration.

## Why `exorcist`
There are many orchestration tools for tasks.
Solutions like [alchemiscale](https://alchemiscale.org/) enable execution of large alchemical networks but requires [running separate infrastructure](https://docs.alchemiscale.org/en/latest/deployment.html).
This project aims to remove this barrier for most users by providing an open and lightweight library for building out simple task orchestration while still allowing for scaling to larger orchestration in the future.

## Architecture
`exorcist` aims to be as minimal as possible in implementation with the high-level goal of ingesting `networkx` DAGs and serializing them into a [`SQLAlchemy`](https://www.sqlalchemy.org/) database.
This means that any SQLAlchemy adapter can hold tasks for execution, in this repo we provide testing for both Postgres and SQLite.
`exorcist` creates two database tables, `tasks` and `dependencies`.

The `tasks` table is the main thing you'll be interested in. It has columns for
the task ID (a string labeling the task), the task status (see
[models.py](https://github.com/OpenFreeEnergy/exorcist/blob/main/exorcist/models.py)),
last modified (which will be not-a-time until the first update), the number of
tries so far, the maximum number tries allowed, and a `task_type` string for
application-defined routing metadata. Exorcist stores `task_type`, but does not
interpret or validate its values.

The `dependencies` table gives details on the dependencies in the DAG. Each entry
in this table has a task ID for the "from" side of the edge and the "to" side
of the edge, as well as a boolean "blocking" column. The "from" task must be
completed before the "to" task can begin. When the "from" task has been
completed, the entry should be updated so the "blocking" is False.

