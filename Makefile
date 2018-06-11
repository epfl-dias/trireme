PLATFORM = $(shell uname -n | tr a-z A-Z)
CC=gcc

.SUFFIXES: .o .c .h

SRC_DIRS = src src/txm src/sm src/bench src/lib/liblatch src/lib/libmsg

CFLAGS += -std=c99
CFLAGS += -Werror
CFLAGS += -g -ggdb
CFLAGS += -march=native -O3 -fno-omit-frame-pointer

CPPFLAGS ?=
CPPFLAGS += -D_GNU_SOURCE -D${PLATFORM}
CPPFLAGS += $(foreach dir, $(SRC_DIRS), -I$(dir)/)

LDFLAGS = -lpthread -lm -lrt -lnuma

SRCS = $(foreach dir, $(SRC_DIRS), $(wildcard $(dir)/*.c))
OBJS = $(SRCS:.c=.o)
DEPS = $(wildcard *.h)

all:trireme

trireme : $(OBJS)
	$(CC) -o $@ $^ $(LDFLAGS)

%.o: %.c $(DEPS)
	$(CC) -c $(CFLAGS) $(CPPFLAGS) -o $@ $<

.PHONY: clean
clean:
	rm -f trireme $(OBJS)
