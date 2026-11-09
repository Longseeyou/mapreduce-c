#include "common.h"
#include <stdint.h>
#include <inttypes.h>
#include <string.h>

static void map(char *filename)
{
    FILE *input = fopen(filename, "r");
    if (!input) { perror(filename); exit(EXIT_FAILURE); }
    char *line = NULL;
    size_t capacity = 0;
    while (getline(&line, &capacity, input) != -1) {
        char *save;
        for (char *word = strtok_r(line, " \t\r\n\v\f", &save); word;
             word = strtok_r(NULL, " \t\r\n\v\f", &save))
            MR_Emit(word, "1");
    }
    if (ferror(input)) fail("Could not read input.");
    free(line);
    fclose(input);
}

static void reduce(char *key, unsigned int partition)
{
    uint64_t count = 0;
    char *value;
    while ((value = MR_GetNext(key, partition)) != NULL) {
        if (count == UINT64_MAX) fail("Word count overflow.");
        ++count;
        free(value);
    }
    /* POSIX stdio locks each call, so output lines do not interleave. */
    if (printf("%s\t%" PRIu64 "\n", key, count) < 0)
        fail("Could not write output.");
}

int main(int argc, char **argv)
{
    return run_example(argc, argv, map, reduce);
}
