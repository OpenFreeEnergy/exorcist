# Exorcist lockfile stress test

Tests to check the stability of lockfiles. This is to test functionality and
scalability of file locks, which will be an essential part of a daemonless
orchestration tool.

## `regan.py`: Common code

This includes core functionality that will be reused between different tasks,
such as creating our simple database and running a task that acquires a given
lockfile and then runs it. Note that `regan.py` can also be run as a script to
test a single lockfile instance. This isn't useful for the stress test, but can
be useful in debugging.

Regan (Linda Blair) was the girl who was possessed in The Exorcist.

## `karras.py`: Result storm test

This test checks what happens when a storm of processes attempts to return
results at once, and have to fight for the lock.

The parameters of the storm are:

* `NRUNS`: The total number of events in the storm
* `STDDEV`: The standard deviation of the Gaussian profile of event times, in
  seconds. This is affects the frequency of events.
* `LOCK_TIME`: The duration that the lock is held for (in addition to the
  duration required to actually obtain the lock in this case). This allows one
  to mock for holding the lock longer than is needed in this example.

Fr. Karras (Jason Miller) is the priest who identifies that Regan is possessed,
and assists in the exorcism during his own crisis of faith.

## `merrin.py`: Death during lock test

Test that we can recover if a process obtains a lock and then dies while
holding the lock. 

Fr. Merrin (Max von Sydow) was the experienced priest who died while performing
Regan's exorcism, and Fr. Kerras had to step in to get the demon out.

## Various lockfiles supported

* `with_fasteners.py` : Use
  [`fasteners`](https://github.com/harlowja/fasteners) for locking

## Our cast


