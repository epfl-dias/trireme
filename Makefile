ARCH   = INTEL64

PLATFORM = $(shell uname -n | tr a-z A-Z)
#PLATFORM = unknown

#optional DFLAGS:
# 	DEBUG, PRINT_PROGRESS, VERIFY_CONSISTENCY (for hash_insert), GATHER_STATS
#
# 	configs: SHARED_EVERYTHING, SHARED_NOTHING
#   latching: SE_LATCH (enables record latching), SE_INDEX_LATCH (hash bucket
#   latching.. enabled by default)
#   lock types:  PTHREAD_SPINLOCK, PTHREAD_MUTEX, ANDERSON_LOCK,
# 				TAS_LOCK, TICKET_LOCK, HTLOCK, RWTICKET_LOCK, CLH_LOCK
# 				RW_LOCK (CUSTOM_RW_LOCK or default=PTHREAD_RW_LOCK)
# 				DRW_LOCK
#
#	CC types: ENABLE_WAIT_DIE_CC ENABLE_NOWAIT_OWNER_CC ENABLE_NOWAIT_CC
#				ENABLE_BWAIT_CC ENABLE_SILO_CC ENABLE_DL_DETECT_CC
#				ENABLE_MVTO ENABLE_MV2PL
#
#	Abort backoff: ENABLE_ABORT_BACKOFF
#
#	key ordering: ENABLE_KEY_SORTING
#
#	migration: MIGRATION (use with NOLATCH because no latching is neccessary,
#				but keep SE_LATCH because if not, locking does not work)
#
# 	trireme-specific options: ENABLE_OP_BATCHING
#
# 	hyperthreading: HT_ENABLED
#
# 	assymetric client--server: ENABLE_ASYMMETRIC_MESSAGING
#
# compile shared everything with se_latch and pthread_spinlock.
# non se_latch config is only for microbenchmark readonly case
#
# SE can also be compiled with other locks. rwlock is only for NOWAIT
#
# compile shared nothing with pthread_spinlock
# compile trireme with nothing
DFLAGS = 
CFLAGS = -march=native -std=c99 -Wfatal-errors -Werror -D_GNU_SOURCE -g -ggdb -O3 -fno-omit-frame-pointer -D$(ARCH) -D$(PLATFORM) $(DFLAGS) #-DSE_INDEX_LATCH
LFLAGS = -lpthread -lm -lrt -lnuma #-ltcmalloc
MAKEDEPEND = gcc -M $(CFLAGS) -o $*.d $<

LIBSRC =  htlock.c ycsb.c smphashtable.c onewaybuffer.c \
				 alock.c tlock.c taslock.c rwticket_lock.c sspinlock.c rwlock.c drwlock.c clh.c\
				 partition.c util.c zipf.c micro_bench.c twopl.c silo.c mvto.c mv2pl.c \
				 ia32msr.c ia32perf.c selock.c \
					plmalloc.c task.c tpcc.c se_dl_detect_graph.c mp_dl_detect_graph.c \
					dreadlock_detect.c svdreadlock.c

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

