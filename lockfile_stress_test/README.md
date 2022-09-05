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

To run `regan.py` with task ID `100`, lock hold time 0.5 seconds, outputting in
file `regan.db`, and using `fasteners` as the underlying lock (via the
`with_fasteners.py` script):

```bash
$ ./regan.py -n 100 -t 0.5 -f regan.db ./with_fasteners.py
Ran task 100, result code was 0
```

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

Run the `karras.py` test with a storm of 1000 events, sampled from a Gaussian
with standard deviation 2.0 seconds, holding each lock for 0.5 seconds, writing
to output file `karras.db` and using `fasteners` for the locking mechanism:

```bash
$ ./karras.py -N 1000 --stddev 2.0 -t 0.5 -f karras.db ./with_fasteners.py
```

## `merrin.py`: Death during lock test

Test that we can recover if a process obtains a lock and then dies while
holding the lock. 

Fr. Merrin (Max von Sydow) was the experienced priest who died while performing
Regan's exorcism, and Fr. Kerras had to step in to get the demon out.

## Various lockfiles supported

* `with_fasteners.py` : Use
  [`fasteners`](https://github.com/harlowja/fasteners) for locking

## Our cast


