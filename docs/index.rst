Exorcist
========

*Task execution/orchestration, without the daemons*

Exorcist is a tool for execution and orchestration of many-task
computing/high-throughput computing.  It is specifically designed for a
common case in the world of simulation, where a large simulation campaign
may include many loosely coupled individual simulations, each of which may
require hours to days to run, and the results need to be gathered into a
large storage backend.

At small to moderate scale, Exorcist can run without setting up any
long-running daemon. At larger scales, Exorcist can interface with standard
database backends, e.g. PostgreSQL. In this way, Exorcist offers the easy
set-up procedure of a daemonless solution, while offering a smooth
transition to a highly scalable solution when needed.

.. toctree::
   :maxdepth: 2
   :caption: Contents:

   guide/index
   api/index



Indices and tables
==================

* :ref:`genindex`
* :ref:`modindex`
* :ref:`search`
