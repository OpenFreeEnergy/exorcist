Introduction to Exorcist
========================

Tasks form a directed acyclic graph (DAG)
-----------------------------------------

Any large-scale campaign can be described as a directed acyclic graph of
tasks, where edges represent the flow of information from an earlier stage
to a later stage. This graph is directed because the outputs of one dask can
be the inputs of a future task, and it is acyclic because it is not possible
for a task to require its own outputs as an input.

Task-based frameworks, like Exorcist, implement efficient methods for
executing these task graphs.

Three databases in Exorcist
---------------------------

The basic idea of Exorcist is that to separate three types of data storage:

* **Task Status Database**: The task status database is the core of
  Exorcist, and fully implemented in Exorcist
* **Task Details Database**: The task details database is a key-value store
  that describes the tasks to be performed; this is specific to the
  application
* **Results Store**: The results store is a generic storage of result data
  from the specific application. The only thing Exorcist needs to know is
  whether the received result was a success or a failure (in which case, it
  can be retried).

Some aspects that distinguish Exorcist from similar tools are the separation
of task status from task descriptions, and the ability for the user to
customize how task descriptions or task status are stored.

Practical usage
---------------

In practice, we tend to think of the user's experience in two stages:
planning the initial campaign, and the running the campaign. 

Planning the campaign
~~~~~~~~~~~~~~~~~~~~~

For many use cases, planning the campaign happens on the user's workstation
or on the head node of a cluster. While this can, in principle, be an
expensive stage, which runs on a compute node, it is more frequently
something that is done interactively by the user.

The "campaign planner" tool must have write access to the task status
database and to the task details database.

Running a campaign: the worker
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

The second 
