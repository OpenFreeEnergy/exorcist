Quickstart
==========

To gain a quick understanding of we ``exorcist`` will build a micro-dashboard using ``pandas``.

Installation
------------
Start by installing ``exorcist`` and ``pandas`` with ``pip``:

.. code-block:: bash

   pip install https://github.com/OpenFreeEnergy/exorcist
   pip install pandas

Or with ``uv``:

.. code-block:: bash

   uv add git+https://github.com/OpenFreeEnergy/exorcist
   uv add pandas


Building a task graph
---------------------
We first need to create an exorcist TaskDB.

.. code-block:: python

   import networkx as nx
   from exorcist import TaskStatus, TaskStatusDB
   import pandas as pd
   # Create a db on disk
   db = TaskStatusDB.from_filename("tasks.db")


In ``exorcist``, we represent task graphs using NetworkX DAGs.
So lets create a few tasks!

.. code-block:: python

   # Edges describe execution order: prerequisite -> dependent task.
   workflow = nx.DiGraph([
       ("download-data", "analyze-data"),
       ("analyze-data", "write-report")
   ])
   db.add_task_network(workflow, max_tries=3)

We can use ``pandas`` to view our tasks:

.. code-block:: python

   tasks = pd.read_sql_table("tasks", db.engine)
   deps = pd.read_sql_table("dependencies", db.engine)
   print(tasks)
   print(deps)

.. code-block:: sh

   TASKS
   =====
             taskid  status last_modified  tries  max_tries task_type
    0  download-data       1           NaT      0          3
    1   analyze-data       0           NaT      0          3
    2   write-report       0           NaT      0          3


   DEPENDENCIES
   ============
             from            to  blocking
    0  download-data  analyze-data      True
    1   analyze-data  write-report      True


Great! We have tasks and dependencies! So lets do some work and complete a task.

.. code-block:: python

   # Pick up the first available task
   task_id = db.check_out_task()
   ... # insert your task is here
   db.mark_task_completed(task_id, success=True)
   tasks = pd.read_sql_table("tasks", db.engine)
   print(tasks)


.. code-block:: sh

   TASKS
   =====
             taskid  status              last_modified  tries  max_tries task_type
    0  download-data      99 2026-08-13 11:21:24.447458      1          3
    1   analyze-data       1 2026-08-13 11:21:24.447549      0          3
    2   write-report       0                        NaT      0          3



As we can see, a task has been completed and now our ``analyze-data`` task is available.

This is the core of how ``exorcist`` works, you check out a task, read the task ID, and do some work with that ID.
If it completes succesfully, mark it as such and the next task becomes available.

For a complete example see `here <https://github.com/OpenFreeEnergy/exorcist/blob/main/examples/quickstart.py>`_.
