ARCH   = INTEL64
DFLAGS = #optional flags include DEBUG, PRINT_PROGRESS, VERIFY_CONSISTENCY (for hash_insert) SHARED_EVERYTHING
CFLAGS := -std=c99 -Wall -D_GNU_SOURCE -fms-extensions -g -O3 -fno-omit-frame-pointer -D$(ARCH) $(DFLAGS)
LFLAGS = -lpthread -lm -lrt -ltcmalloc
MAKEDEPEND = gcc -M $(CFLAGS) -o $*.d $<

LIBSRC =  ycsb.c smphashtable.c onewaybuffer.c \
				 partition.c util.c zipf.c tpcc.c micro_bench.c \
				 ia32msr.c ia32perf.c selock.c

LIBOBJS = $(LIBSRC:.c=.o)

BINS = ycsb

all: $(BINS)

ycsb: %: %.o $(LIBOBJS)
	g++ -o $@ $^ $(LFLAGS)

%.P : %.c
				-$(MAKEDEPEND)
				-@sed 's/\($*\)\.o[ :]*/\1.o $@ : /g' < $*.d > $@; \
					rm -f $*.d; [ -s $@ ] || rm -f $@

include $(SRCS:.c=.P)

.PHONY: clean
clean: 
	rm -f $(BINS) *.o *.P

