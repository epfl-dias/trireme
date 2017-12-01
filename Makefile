ARCH   = INTEL64

PLATFORM = $(shell uname -n | tr a-z A-Z)

CC=gcc

DFLAGS +=
CFLAGS = -march=native -std=c99 -Wfatal-errors -Werror -D_GNU_SOURCE -g -ggdb -O3 -fno-omit-frame-pointer -D$(ARCH) -D$(PLATFORM) $(DFLAGS)
LFLAGS = -lpthread -lm -lrt -lnuma

DEPS = $(wildcard *.h)

.SUFFIXES: .o .c .h

SRCS = $(wildcard *.c)
OBJS = $(SRCS:.c=.o)

all:trireme

trireme : $(OBJS)
	$(CC) -o $@ $^ $(LFLAGS)

%.o: %.c $(DEPS)
	$(CC) -c $(CFLAGS) $(DFLAGS) -o $@ $<

.PHONY: clean
clean:
	rm -f trireme $(OBJS)
