import networkx as nx
from exorcist import TaskStatus, TaskStatusDB
import pandas as pd

# Create a db on disk
db = TaskStatusDB.from_filename("tasks.db")


# Edges describe execution order: prerequisite -> dependent task.
workflow = nx.DiGraph(
    [("download-data", "analyze-data"), ("analyze-data", "write-report")]
)
db.add_task_network(workflow, max_tries=3)

tasks = pd.read_sql_table("tasks", db.engine)
deps = pd.read_sql_table("dependencies", db.engine)
print("TASKS")
print("=====")
print(tasks.to_string())
print("\n")
print("DEPENDENCIES")
print("============")
print(deps.to_string())
print("\n")


# Pick up the first available task
task_id = db.check_out_task()
...  # insert your task here
db.mark_task_completed(task_id, success=True)
tasks = pd.read_sql_table("tasks", db.engine)
print("TASKS")
print("=====")
print(tasks)
