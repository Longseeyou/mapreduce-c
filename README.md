# MapReduce C

**A reusable, multithreaded MapReduce library in C.**

Supply a mapper and a reducer; the library schedules file inputs, groups emitted
string pairs by key, and runs reductions across a fixed POSIX thread pool.
Use it for word frequencies, grouped totals, and other file-based aggregations.

C11 · POSIX threads · Static library · Linux / macOS / WSL

```text
Input files       Map callbacks       Partition + group       Reduce callbacks
words.txt   ───┐                      "map"    → [1, 1]         "map"    → 2
more.txt    ───┴──→ MR_Emit(key, value) "reduce" → [1, 1]   ───→ "reduce" → 2
```

## Build and try it

You need a C compiler and Make. On Windows, run these commands inside WSL.
Tests also need Python 3.9 or newer; the sanitizer target uses Clang.

```sh
make
./build/wordcount -j 4 -p 8 examples/words.txt | sort
# map       2
# partition 1
# reduce    2
# threads   1

./build/sum_by_key examples/amounts.tsv | sort
# books     20
# music     6
```

Both programs print tab-separated rows to stdout. `-j` selects workers (default
4), `-p` selects partitions (default 8), and `-h` prints usage. Pass any number
of files; use `--` before filenames that begin with `-`. Redirect stdout to save
results. Output order across partitions is unspecified; pipe through `sort` when
you want a stable display.

Word count splits on ASCII whitespace, ignores empty tokens, and preserves case
and punctuation. Sum-by-key accepts exactly two whitespace-separated fields per
nonblank line: a key and a finite number. Floating-point summation order may vary,
so small rounding differences are possible. Both examples terminate with an error
if input/output fails; output can be partial on failure.

## Use the library

The build produces `build/libmapreduce.a`. Include `include/mapreduce.h`, link
the archive, and enable pthreads:

```sh
cc -std=c11 -D_POSIX_C_SOURCE=200809L -Iinclude app.c \
   build/libmapreduce.a -pthread -o app
```

A reducer pulls values for one key until the iterator returns `NULL`:

```c
#include "mapreduce.h"
#include <stdio.h>
#include <stdlib.h>

static void reduce(char *key, unsigned int partition)
{
    unsigned long count = 0;
    char *value;
    while ((value = MR_GetNext(key, partition)) != NULL) {
        ++count;
        free(value);
    }
    printf("%s\t%lu\n", key, count);
}

/* In your mapper, call MR_Emit(key, value) for each intermediate pair.
 * Then from main, with your map callback and input filenames:
 * MR_Status status = MR_Run(file_count, files, map, reduce, 4, 8);
 * Check status against MR_OK; MR_StatusString(status) explains failures.
 */
```

See [wordcount.c](examples/wordcount.c) and [sum_by_key.c](examples/sum_by_key.c)
for complete applications using the same engine for different aggregations.

| API | Contract |
| --- | --- |
| `MR_Run(...)` | Blocks until work and cleanup finish; returns `MR_Status` |
| `MR_Emit(key, value)` | Mapper-only; copies both NUL-terminated strings |
| `MR_GetNext(key, partition)` | Reducer-only; returns an owned value to `free`, or `NULL` |
| `MR_Partitioner(key, count)` | DJB2 hash; requires a non-null key and positive count |
| `MR_StatusString(status)` | Human-readable framework status |

Mapper filenames and reducer keys are borrowed and must not be modified or freed.
Reducers run once per distinct key, and each partition has one reduce job.
Unconsumed values are freed when the callback returns. Callbacks may execute in
parallel, so synchronize shared application state and output. Emit only during
mapping; retrieve values only for the reducer's current key and partition.
Callbacks must return normally and must not leave background work using the API.

`MR_Run` returns `MR_INVALID_ARGUMENT`, `MR_INPUT_ERROR`, `MR_RESOURCE_ERROR`, or
`MR_BUSY` on framework failures. Input paths must name regular files. Callback
I/O errors are application-owned; a successful framework status cannot report
them. Allocation failure during emit or reduce-key copying calls `abort()`.

## How it works

1. Validate input metadata and submit map jobs in ascending file-size order.
2. Copy emitted pairs into hash-selected, mutex-protected partitions. Sorted
   linked lists group equal keys together.
3. Wait until the map queue and all active mapper callbacks have finished.
4. Submit partitions in ascending stored-byte order and reduce each key.
5. Wait for reducers, join workers, and release storage.

The original shortest-job-first submission policy is preserved. OS scheduling
can change callback start and finish order. Queue insertion is constant time;
the same pool serves both phases. See [architecture](docs/ARCHITECTURE.md) for
synchronization, ownership, and tradeoffs.

## Tests

```sh
make test
make sanitize
# Optional deeper diagnostics on Linux:
valgrind --leak-check=full --error-exitcode=1 ./build/test_mapreduce
valgrind --tool=helgrind --error-exitcode=1 ./build/test_mapreduce
```

The C suite checks 27 aggregation runs across worker/partition combinations,
phase ordering, copied emission buffers, exactly-once key reduction, repeated
runs, skipped values, invalid inputs, and nested/overlapping run rejection.
Python checks both applications against independently computed results, including
empty input, whitespace, UTF-8 tokens, numeric aggregation, and invalid CLI/data.
An optional [GitHub Actions template](docs/github-actions.yml) runs the suite on
Ubuntu and macOS and runs Linux ASan/UBSan. To activate it, copy the template to
`.github/workflows/ci.yml` and push with credentials permitted to write workflows.
The template is included as documentation; hosted CI is not currently enabled.

## Layout

```text
include/mapreduce.h      Public callback API
src/mapreduce.c          Partition storage and phase scheduling
src/threadpool.[ch]      Internal worker pool
examples/               Word-count and sum-by-key applications + small inputs
tests/                  C API and Python CLI correctness checks
docs/                   Architecture and design limits
```

## Scope and background

This is an in-process, in-memory engine. It does not provide distributed workers,
network transport, disk spilling, fault recovery, custom partitioners, or binary
keys/values. Sorted linked-list insertion can be quadratic for many distinct
keys. One run can be active per process; simultaneous or nested calls return
`MR_BUSY`. Sequential runs are supported. Native Windows is not supported.

Created by **Nguyen Hoang Long Dinh**, developed from a CMPUT 379 MapReduce
implementation into a standalone library. The core partition and worker-pool
design is retained, with status reporting, reusable build output, two independent
examples, and automated validation. See [NOTICE.md](NOTICE.md) for provenance.
