#include "common.h"
#include <math.h>
#include <string.h>

static double number(const char *text)
{
    char *end;
    errno = 0;
    double result = strtod(text, &end);
    if (errno || end == text || *end || !isfinite(result))
        fail("Expected a finite numeric value.");
    return result;
}

static void map(char *filename)
{
    FILE *input = fopen(filename, "r");
    if (!input) { perror(filename); exit(EXIT_FAILURE); }
    char *line = NULL;
    size_t capacity = 0;
    while (getline(&line, &capacity, input) != -1) {
        char *save;
        char *key = strtok_r(line, " \t\r\n\v\f", &save);
        if (!key) continue;
        char *value = strtok_r(NULL, " \t\r\n\v\f", &save);
        if (!value || strtok_r(NULL, " \t\r\n\v\f", &save))
            fail("Expected two fields per line: key number.");
        (void)number(value);
        MR_Emit(key, value);
    }
    if (ferror(input)) fail("Could not read input.");
    free(line);
    fclose(input);
}

static void reduce(char *key, unsigned int partition)
{
    double sum = 0;
    char *value;
    while ((value = MR_GetNext(key, partition)) != NULL) {
        sum += number(value);
        free(value);
        if (!isfinite(sum)) fail("Sum overflow.");
    }
    if (printf("%s\t%.17g\n", key, sum) < 0)
        fail("Could not write output.");
}

int main(int argc, char **argv)
{
    return run_example(argc, argv, map, reduce);
}
