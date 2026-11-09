#ifndef EXAMPLE_COMMON_H
#define EXAMPLE_COMMON_H

#include "mapreduce.h"
#include <errno.h>
#include <limits.h>
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>

static void fail(const char *message)
{
    fprintf(stderr, "%s\n", message);
    exit(EXIT_FAILURE);
}

static unsigned int positive_number(const char *text)
{
    char *end;
    errno = 0;
    unsigned long value = strtoul(text, &end, 10);
    if (errno || text[0] < '0' || text[0] > '9' || *end ||
        value == 0 || value > UINT_MAX)
        fail("Worker and partition counts must be positive integers.");
    return (unsigned int)value;
}

static int run_example(int argc, char **argv, Mapper mapper, Reducer reducer)
{
    unsigned int workers = 4, partitions = 8;
    int option;
    while ((option = getopt(argc, argv, "hj:p:")) != -1) {
        switch (option) {
        case 'j': workers = positive_number(optarg); break;
        case 'p': partitions = positive_number(optarg); break;
        case 'h':
            printf("Usage: %s [-j workers] [-p partitions] [--] file...\n", argv[0]);
            return EXIT_SUCCESS;
        default: return EXIT_FAILURE;
        }
    }
    if (optind == argc)
        fail("Provide at least one input file; use -h for help.");
    MR_Status status = MR_Run((unsigned int)(argc - optind), argv + optind,
                              mapper, reducer, workers, partitions);
    if (status != MR_OK) {
        fprintf(stderr, "MapReduce: %s\n", MR_StatusString(status));
        return EXIT_FAILURE;
    }
    if (fflush(stdout) == EOF)
        fail("Could not write output.");
    return EXIT_SUCCESS;
}

#endif
