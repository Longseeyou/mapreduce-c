# Architecture

## Execution model

`MR_Run` owns the complete lifecycle. A process-wide try-lock protects the global
partition array from overlapping controllers. It returns `MR_BUSY` immediately
for another active run, including calls made from a callback. The controller
initializes partitions and workers, completes mapping, completes reduction, then
destroys the pool and partitions before releasing the run lock.

All input paths are checked before any mapper is submitted. Files may still
change or become unreadable afterward; callbacks handle opening and reading.
Zero input files are allowed by the library and produce no reducer callbacks.

## Scheduling and synchronization

Each phase sorts lightweight job descriptions, never partitions or their mutexes.
Map jobs use file byte size; reduce jobs use emitted key/value string bytes,
including terminators. Both submit in ascending size order. This preserves the
original shortest-job-first policy, which is a heuristic rather than a guarantee
of the fastest overall completion time.

The worker queue is FIFO with head and tail pointers. Its mutex protects queued
and active counts, links, and shutdown state. Workers remove jobs and increment
the active count under the same lock, execute callbacks without that lock, then
decrement the active count. `ThreadPool_check` waits on a condition variable until
both counts are zero. Condition predicates are always checked in loops.

The map/reduce boundary therefore waits for active callbacks as well as queued
work. Destruction drains work, signals shutdown, joins workers, and destroys
condition variables and the mutex. The thread-pool header is internal; it is not
part of the supported library API.

## Partitions and ownership

The DJB2 hash assigns equal keys to one partition. Each partition has a sorted
singly linked list and its own mutex. Emission duplicates both strings before
inserting a node under the partition lock. Different partitions permit parallel
insertion, but a hot key causes contention and many distinct keys cause long
linear insertion scans.

Reduction starts only after mapping ends. Exactly one job owns each partition
during reduction. It copies the next key and invokes the application without
holding the partition mutex. `MR_GetNext` removes a matching head node and frees
the node/key; the value becomes the caller's responsibility. The callback key
remains valid until it returns. Remaining values for that key are discarded,
ensuring progress even if a reducer intentionally stops early.

Job argument arrays remain alive until all submitted jobs finish, including on
submission failure. The controller performs final pool and partition cleanup.
Resource failure during scheduling can leave partial application side effects;
there is no rollback. Emit and reduce-key allocation failures retain the original
fail-fast `abort()` behavior because the callback API has no error return channel.

## Changes from the original implementation

- Public header now documents callback lifetimes and thread-safety obligations.
- `MR_Run` returns a status; existing callers may ignore it, but checking it is
  recommended. The internal reduce-job entry point is no longer public.
- Overlapping and nested runs return `MR_BUSY` instead of corrupting globals.
- Invalid paths, directory inputs, and null filenames are reported before mapping.
- A reducer that leaves values unread cannot repeatedly receive the same key.
- The thread-pool queue has a tail pointer for constant-time insertion.
- Hashing treats string bytes as unsigned, making non-ASCII byte handling explicit.
- Word-count and numeric-aggregation examples share the same library and CLI options.

## Limits

All intermediate strings live in memory. Values have no ordering guarantee;
reduction order across partitions is also unspecified. String hashes may differ
between platforms with different `unsigned long` widths. Partition IDs therefore
are not a portable persistent format.

Callbacks cannot attach per-run user data through this API. They must manage any
shared application state and synchronization themselves. There is no cancellation,
retry, recovery from callback termination, binary record support, or simultaneous
independent runtime contexts. Use separate processes for independent concurrent
runs or extend the API with explicit contexts before adding that capability.
