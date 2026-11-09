CC = cc
AR = ar
CPPFLAGS = -Iinclude -D_POSIX_C_SOURCE=200809L
CFLAGS = -std=c11 -O2 -g -Wall -Wextra -Wpedantic -Werror
LDLIBS = -pthread
BUILD ?= build

.PHONY: all test sanitize clean
all: $(BUILD)/libmapreduce.a $(BUILD)/wordcount $(BUILD)/sum_by_key

$(BUILD):
	mkdir -p $@

$(BUILD)/mapreduce.o: src/mapreduce.c include/mapreduce.h src/threadpool.h | $(BUILD)
	$(CC) $(CPPFLAGS) $(CFLAGS) -pthread -c $< -o $@

$(BUILD)/threadpool.o: src/threadpool.c src/threadpool.h | $(BUILD)
	$(CC) $(CPPFLAGS) $(CFLAGS) -pthread -c $< -o $@

$(BUILD)/libmapreduce.a: $(BUILD)/mapreduce.o $(BUILD)/threadpool.o
	$(AR) rcs $@ $^

$(BUILD)/wordcount: examples/wordcount.c examples/common.h $(BUILD)/libmapreduce.a
	$(CC) $(CPPFLAGS) $(CFLAGS) $(LDFLAGS) $< $(BUILD)/libmapreduce.a $(LDLIBS) -o $@

$(BUILD)/sum_by_key: examples/sum_by_key.c examples/common.h $(BUILD)/libmapreduce.a
	$(CC) $(CPPFLAGS) $(CFLAGS) $(LDFLAGS) $< $(BUILD)/libmapreduce.a $(LDLIBS) -o $@

$(BUILD)/test_mapreduce: tests/test_mapreduce.c $(BUILD)/libmapreduce.a
	$(CC) $(CPPFLAGS) $(CFLAGS) $(LDFLAGS) $< $(BUILD)/libmapreduce.a $(LDLIBS) -o $@

test: all $(BUILD)/test_mapreduce
	./$(BUILD)/test_mapreduce
	python3 tests/test_examples.py $(BUILD)

sanitize:
	$(MAKE) BUILD=build/sanitize CC=clang CFLAGS='-std=c11 -O1 -g -Wall -Wextra -Wpedantic -Werror -fsanitize=address,undefined -fno-omit-frame-pointer' test

clean:
	rm -rf build
