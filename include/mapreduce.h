#ifndef MAPREDUCE_H
#define MAPREDUCE_H

/* File-oriented, in-process MapReduce over NUL-terminated string pairs. */
typedef void (*Mapper)(char *file_name);
typedef void (*Reducer)(char *key, unsigned int partition_idx);

typedef enum {
    MR_OK = 0,
    MR_INVALID_ARGUMENT,
    MR_INPUT_ERROR,
    MR_RESOURCE_ERROR,
    MR_BUSY
} MR_Status;

/* Blocks through both phases. One run at a time; overlapping/nested runs
 * return MR_BUSY. Empty input is valid. File names live until this returns.
 * Callbacks execute concurrently and must synchronize their own shared data.
 * Status covers framework setup/scheduling, not application callback errors.
 */
MR_Status MR_Run(unsigned int file_count, char *file_names[],
                 Mapper mapper, Reducer reducer,
                 unsigned int num_workers, unsigned int num_parts);

/* Mapper-only. Copies both strings; allocation failure terminates via abort. */
void MR_Emit(char *key, char *value);

/* Reducer-only, for the current key and partition. Returns an owned string
 * (caller must free), or NULL at end. Key passed to reducer is borrowed.
 * Unconsumed values are discarded after the reducer returns.
 */
char *MR_GetNext(char *key, unsigned int partition_idx);

/* DJB2 string hash. Requires key != NULL and num_partitions > 0. */
unsigned int MR_Partitioner(char *key, unsigned int num_partitions);
const char *MR_StatusString(MR_Status status);

#endif
