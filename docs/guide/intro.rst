Introduction to Exorcist
========================

Tasks form a directed acyclic graph (DAG)
-----------------------------------------

Any large-scale campaign can be described as a directed acyclic graph of
tasks, where edges represent the flow of information from an earlier stage
to a later stage. This graph is directed because the outputs of one task can
be the inputs of a future task, and it is acyclic because it is not possible
for a task to require its own outputs as an input.

Task-based frameworks, like Exorcist, implement efficient methods for
executing these task graphs.

Three databases in Exorcist
---------------------------

The central idea of Exorcist is to separate three types of data storage:

* **Task Status Database**: The task status database is the core of
  Exorcist, and fully implemented in Exorcist. This database contains the
  task identifiers, and information on the execution status of those tasks.
  It is intentionally small and simple, to enable better concurrency.
* **Task Details Database**: The task details database is a key-value store
  that describes the tasks to be performed; this is specific to the
  client application. The client must define how these tasks are serialized
  and deserialized into the database.
* **Results Store**: The results store is a generic storage of result data
  from the specific application. The only thing Exorcist needs to know is
  whether the received result was a success or a failure (in which case,
  Exorcist can take responsibility for ensuring that it gets retried).

Some aspects that distinguish Exorcist from similar tools are the separation
of task status from task descriptions, and the ability for the user to
customize how task descriptions or task status are stored.

Practical usage for users
-------------------------

When thinking about the end user's experience, Exorcist tends to assume that
this occurs in two stages: planning the initial campaign, and the running
the campaign. The general assumption is that these will beqtwo different
software tools (typically two executables) with different user experiences.

Preparing the campaign: the planner
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

For many use cases, planning the campaign happens on the user's workstation
or on the head node of a cluster. While this can, in principle, be a stage
that must run on a compute node, it is more frequently something that is
done interactively by the user.

The "campaign planner" tool must have write access to the *task status
database* and to the *task details database*.


Running a campaign: the worker
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

The second tool that the user interacts with is the worker. Typically, the
user will launch many identical workers through jobs submitted to their
queueing system.

The worker is the workhorse of the campaign. It does the real computational
effort. It needs read/write access to the *task status database*, read access
to the *task details database*, and write access to the *results store*.

In most cases, it will also have read access to the results store, which
allows things like analysis as a separate task. If it can also have write
access to the the task details database, this allows on-the-fly creation of
new tasks, e.g., focusing simulation effort in a different direction based
on the results of the campaign so far.
