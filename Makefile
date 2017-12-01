PLATFORM = $(shell uname -n | tr a-z A-Z)
CC=gcc

.SUFFIXES: .o .c .h

SRC_DIRS = ./ ./txm ./sm ./bench ./lib/liblatch ./lib/libmsg 
INCLUDE = $(foreach dir, $(SRC_DIRS), -I$(dir)/)
SRCS = $(foreach dir, $(SRC_DIRS), $(wildcard $(dir)/*.c))
OBJS = $(SRCS:.c=.o)
DEPS = $(wildcard *.h)

DFLAGS =
CFLAGS = -march=native -std=c99 -Werror -D_GNU_SOURCE -g -ggdb -O3 -fno-omit-frame-pointer
CFLAGS += -D$(PLATFORM) $(DFLAGS) $(INCLUDE) 
LFLAGS = -lpthread -lm -lrt -lnuma

all:trireme

trireme : $(OBJS)
	$(CC) -o $@ $^ $(LFLAGS)

%.o: %.c $(DEPS)
	$(CC) -c $(CFLAGS) $(DFLAGS) -o $@ $<

.PHONY: clean
clean:
	rm -f trireme $(OBJS)
