[![DOI](https://zenodo.org/badge/DOI/10.5281/zenodo.14740961.svg)](https://doi.org/10.5281/zenodo.14740961)

# exorcist
Daemonless campaign-scale simulation orchestration


### Micro-dashboard (pandas DataFrame)

Eventually, we expect to develop a useful dashboard. Until then, the most
convenient way for users to get an overview of the task status database is by
using `pandas` (version 2.0 or greater):

```python
import pandas as pd
from exorcist import TaskStatusDB

db = TaskStatusDB.from_filename("path/to/my/database.db")
tasks = pd.read_sql_table("tasks", db.engine)
deps = pd.read_sql_table("dependencies", db.engine)
```

NOTE: `pandas` is not in the `exorcist` requirement stack, so you may need to
install it separately.

The tasks table is the main thing you'll be interested in. It has columns for
the task ID (a string labeling the task), the task status (see
[models.py](https://github.com/OpenFreeEnergy/exorcist/blob/main/exorcist/models.py)),
last modified (which will be not-a-time until the first update), the number of
tries so far, and the maximum number tries allowed.

The dependencies table gives details on the dependencies in the DAG. Each entry
in this table has a task ID for the "from" side of the edge and the "to" side
of the edge, as well as a boolean "blocking" column. The "from" task must be
completed before the "to" task can begin. When the "from" task has been
completed, the entry should be updated so the "blocking" is False.
