# MapReduce C

A standalone MapReduce library in C by **Nguyen Hoang Long Dinh**, inspired by
systems programming concepts explored in CMPUT 379.

The library processes files in parallel using POSIX threads. Applications supply
two functions: a **mapper** that emits key-value pairs and a **reducer** that
combines the values belonging to each key. The library handles scheduling,
partitioning, synchronization, and intermediate storage.

## How MapReduce works

Consider two files containing these words:

```text
File A: apple banana apple
File B: banana orange
```

The application chooses what each stage means. For word counting, the mapper
emits a value of `"1"` for every word, and the reducer counts the values:

```text
MAP                         GROUP BY KEY                 REDUCE
("apple", "1")   ------>    "apple":  ["1", "1"]  ---->  apple   2
("banana", "1")  ------>    "banana": ["1", "1"]  ---->  banana  2
("apple", "1")              "orange": ["1"]       ---->  orange  1
("banana", "1")
("orange", "1")
```

The engine treats keys and values as strings. It has no word-counting logic:
changing the callbacks lets the same engine calculate grouped totals or other
aggregations. The included `sum_by_key` example demonstrates this by adding
numeric values for each key.

## Inside the engine

### 1. Schedule input files

`MR_Run` validates the input paths, creates the partitions, and starts a fixed
thread pool. Each input file becomes one map job. Jobs are submitted in ascending
file-size order, so smaller waiting jobs are considered first.

Workers take jobs from a shared FIFO queue. A mutex protects the queue, while a
condition variable lets idle workers sleep until work arrives. A tail pointer
makes adding a job constant time. Thread scheduling can still change the order
in which callbacks start or finish.

### 2. Map and partition

Each mapper reads its file and calls `MR_Emit(key, value)`. The library copies
both strings, so the mapper can reuse its input buffers immediately.

A DJB2 hash selects a partition for each key. Equal keys always reach the same
partition within a run. Each partition stores pairs in a linked list sorted by
key, keeping matching keys together. Its own mutex protects concurrent inserts;
workers inserting into different partitions can proceed independently.

### 3. Wait for mapping to finish

The controller waits until the work queue is empty **and** no workers are still
running map callbacks. An empty queue alone would not be sufficient: a worker
may already have taken a job and still be emitting pairs.

This phase boundary ensures that reducers see all emitted values.

### 4. Reduce each key

The engine submits one reduce job per partition, in ascending order of stored
key/value bytes. It reuses the same worker pool.

Within a partition, the engine invokes the reducer once for each distinct key.
The reducer repeatedly calls `MR_GetNext(key, partition)` to retrieve its values.
Each returned string belongs to the application and must be freed. Values left
unread are discarded when the callback returns.

Different partitions can reduce concurrently. There is no guaranteed output
order across partitions, so applications must synchronize shared state or output
as needed.

### 5. Clean up

After all reduce jobs finish, the engine joins the workers, destroys its
synchronization objects, and frees intermediate storage. `MR_Run` then returns
a status to the application. A later call starts a fresh run.

## Build and run

Use a C compiler and Make on Linux, macOS, or Windows through WSL.

```sh
make
./build/wordcount -j 4 -p 8 examples/words.txt | sort
```

Output, with tab-separated columns:

```text
map         2
partition   1
reduce      2
threads     1
```

Run the numeric aggregation example:

```sh
./build/sum_by_key examples/amounts.tsv | sort
```

```text
books   20
music   6
```

Both examples accept multiple input files. `-j` sets the number of workers
(default 4), `-p` sets the number of partitions (default 8), and `-h` shows usage.
Use `--` before filenames beginning with `-`. Results go to stdout; use shell
redirection to save them.

Word count splits on ASCII whitespace and preserves case and punctuation.
Sum-by-key expects a key and a finite number on each nonblank line, separated
by whitespace. Floating-point rounding can vary with value order. Both examples
exit on input/output errors; output may be partial if a run fails.

## Write your own application

Include `mapreduce.h` and implement the two callbacks:

- **Mapper:** read a file and call `MR_Emit` for each intermediate pair.
- **Reducer:** consume the current key's values with `MR_GetNext` and produce
  the application's result.

Start the engine with your callbacks:

```c
MR_Status status = MR_Run(file_count, files, map, reduce, 4, 8);
if (status != MR_OK) {
    fprintf(stderr, "%s\n", MR_StatusString(status));
}
```

The build produces a static library. Link it into your application with pthreads:

```sh
cc -std=c11 -D_POSIX_C_SOURCE=200809L -Iinclude app.c \
   build/libmapreduce.a -pthread -o app
```

See [wordcount.c](examples/wordcount.c) and [sum_by_key.c](examples/sum_by_key.c)
for complete mapper, reducer, and application examples.

| Function | Purpose |
| --- | --- |
| `MR_Run(...)` | Execute both phases, clean up, and return a framework status |
| `MR_Emit(key, value)` | Copy a string pair into intermediate storage during mapping |
| `MR_GetNext(key, partition)` | Return the next owned value, or `NULL` when exhausted |
| `MR_Partitioner(key, count)` | Calculate a partition index for a non-null key and positive count |
| `MR_StatusString(status)` | Describe a framework status |

Mapper filenames and reducer keys are borrowed: do not modify or free them.
Use `MR_Emit` only inside mappers and `MR_GetNext` only for the reducer's current
key and partition. Callbacks must return normally and finish all work using the
API before returning.

The engine reports invalid arguments, invalid input paths, setup/scheduling
resource failures, and overlapping runs. Callbacks handle their own I/O errors.
Allocation failure while copying emitted pairs or reducer keys calls `abort()`.

## Design limits

All intermediate data stays in memory on one machine. This engine has no network
workers, disk spilling, or retry mechanism. Keys and values are NUL-terminated
strings, and inputs must be regular files.

One run may be active per process. Overlapping or nested calls return `MR_BUSY`;
sequential runs are supported. Sorted linked-list insertion can become quadratic
with many distinct keys, and frequently emitted keys can cause partition-lock
contention. More workers therefore do not always mean faster execution.

## Project layout

```text
include/mapreduce.h     Public API and callback contracts
src/mapreduce.c         Partition storage and map/reduce phases
src/threadpool.[ch]     Worker queue, waiting, and shutdown
examples/              Word-count and numeric-aggregation applications
tests/                 API and application tests
docs/                  Additional architecture notes and CI template
```

## Verify the implementation

```sh
make test       # C API checks and Python example tests
make sanitize   # The same suite with Clang AddressSanitizer and UBSan
```

Tests require Python 3.9 or newer; the sanitizer target also requires Clang.
The suite checks aggregation across different worker and partition counts,
phase ordering, memory ownership, repeated runs, invalid input, overlapping
run rejection, and both example applications.
