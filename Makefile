ARCH   = INTEL64

#optional DFLAGS: DEBUG, PRINT_PROGRESS, VERIFY_CONSISTENCY (for hash_insert), GATHER_STATS
# 								configs: SHARED_EVERYTHING, SHARED_NOTHING
# 								latching: SE_LATCH (enables latching in shared everything)
# 								lock types:  PTHREAD_SPINLOCK, PTHREAD_MUTEX, ANDERSON_LOCK, TICKET_LOCK
# 								CC types: ENABLE_WAIT_DIE_CC
# 								trireme-specific options: ENABLE_OP_BATCHING, PARTITION_LOCK_MODE 
# compile shared everything with se_latch and pthread_spinlock.
# non se_latch config is only for microbenchmark readonly case
# 
# compile shared nothing with pthread_spinlock
# compile trireme with nothing
DFLAGS =
CFLAGS := -march=native -std=c99 -Wfatal-errors -Werror -D_GNU_SOURCE -g -ggdb -O2 -fno-omit-frame-pointer -D$(ARCH) $(DFLAGS)
LFLAGS = -lpthread -lm -lrt #-ltcmalloc 
MAKEDEPEND = gcc -M $(CFLAGS) -o $*.d $<

LIBSRC =  ycsb.c smphashtable.c onewaybuffer.c \
				 partition.c util.c zipf.c micro_bench.c twopl.c \
				 ia32msr.c ia32perf.c selock.c alock.c tlock.c plmalloc.c task.c #tpcc.c

LIBOBJS = $(LIBSRC:.c=.o)

BINS = ycsb

all: $(BINS)

ycsb: %: %.o $(LIBOBJS)
	gcc -o $@ $^ $(LFLAGS)

%.P : %.c
				-$(MAKEDEPEND)
				-@sed 's/\($*\)\.o[ :]*/\1.o $@ : /g' < $*.d > $@; \
					rm -f $*.d; [ -s $@ ] || rm -f $@

include $(SRCS:.c=.P)

.PHONY: clean
clean: 
	rm -f $(BINS) *.o *.P

