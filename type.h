#ifndef __TYPE_H__
#define __TYPE_H__

#include <sys/queue.h>
#include <inttypes.h>

/**
 * hash_key - Hash table key type
 */
typedef uint64_t hash_key;

/* lock used to implement 2pl wait die */
struct lock_entry {
  int s;
  uint64_t ts;
  char optype;
  volatile char ready;
  TAILQ_ENTRY(lock_entry) next;
};

TAILQ_HEAD(lock_list, lock_entry);

/* lock type used to implement latching */
#if ANDERSON_LOCK

// anderson lock
struct myalock {
  volatile int x;
} __attribute__ ((aligned (CACHELINE)));

typedef struct {
  volatile struct myalock has_lock[MAX_SERVERS] __attribute__ ((aligned (CACHELINE)));
  volatile int next_slot;
  volatile int nthread;
} __attribute__ ((aligned (CACHELINE))) alock_t;

#define LATCH_T alock_t
#define LATCH_INIT alock_init
#define LATCH_ACQUIRE alock_acquire
#define LATCH_RELEASE alock_release
#elif TICKET_LOCK

typedef struct
{
  volatile unsigned short ticket;
  volatile unsigned short users;
} tlock_t;

#define LATCH_T tlock_t
#define LATCH_INIT(latch, nservers) tlock_init(latch)
#define LATCH_ACQUIRE(latch, state) tlock_acquire(latch)
#define LATCH_RELEASE(latch, state) tlock_release(latch)
#elif PTHREAD_SPINLOCK
#define LATCH_T pthread_spinlock_t
#define LATCH_INIT(latch, nservers) pthread_spin_init(latch, 0)
#define LATCH_ACQUIRE(latch, state) pthread_spin_lock(latch)
#define LATCH_RELEASE(latch, state)  pthread_spin_unlock(latch)
#else
#define LATCH_T pthread_mutex_t
#define LATCH_INIT(latch, nservers) pthread_mutex_init(latch, NULL)
#define LATCH_ACQUIRE(latch, state) pthread_mutex_lock(latch)
#define LATCH_RELEASE(latch, state)  pthread_mutex_unlock(latch)
#endif

/* base elem used to represent a tuple */
struct elem {
  // size must be 64 bytes
  hash_key key;
  size_t size;
  uint64_t ref_count;
  TAILQ_ENTRY(elem) chain;
  char *value;
  uint64_t local_values[1];
#if SHARED_EVERYTHING
  LATCH_T latch;
#if ENABLE_WAIT_DIE_CC
  struct lock_list waiters; 
  struct lock_list owners;
#endif
#endif
} __attribute__ ((aligned (CACHELINE)));

TAILQ_HEAD(elist, elem);

/* plmalloc dses 
 * Each partition, and hence each thread has a dedicated heap. Each heap
 * contains an array of buckets. Each bucket is a linked list, one list per 
 * tuple type. The max #tuple-types we have is 10 for TPCC. For microbench it is
 * just 1. Each list links mem_tuple elements. Each mem_tuple
 *
 * We separate out storage of tuple data from space for storing elem structs
 * as 1) elem structs are small and it is a waste of space to use an 
 * additional mem_tuple metadata structure to wrap each elem struct, and
 * 2) elem structs are cache aligned
 * 
 */
struct mem_tuple {
  char *data;
  LIST_ENTRY(mem_tuple) next;
} __attribute__ ((aligned (CACHELINE)));

LIST_HEAD(tlist, mem_tuple);

struct mem_bucket {
  size_t tsize; // size of tuples stored in this bucket
  int prealloc_cnt;
  struct tlist list;
};

struct mem_heap {
  int nslabs;
  struct tlist base_ptrs;
  struct elist efree_list; /* to hold elem structs */
  struct mem_bucket slab[MAX_TUPLE_TYPES]; /* to hold actual tuples */
} __attribute__ ((aligned (CACHELINE)));

/**
 * Hash Table Storage Data Structures
 * struct bucket     - a bucket in a partition
 * struct partition  - hash table partition for server
 */
struct bucket {
#if SE_LATCH
  LATCH_T latch;
#endif
  struct elist chain;
}__attribute__ ((aligned (CACHELINE)));

struct op_ctx {
  char is_local;
  char optype; 
  struct elem *e;
  void *old_value;
};

struct txn_ctx {
  uint64_t ts;
  int nops;
  struct op_ctx op_ctx[MAX_OPS_PER_QUERY];
};

#if SHARED_EVERYTHING
typedef enum sethread_state {
  STATE_WAIT, 
  STATE_READY
} sethread_state_t;
#endif

struct partition {
  int nservers;
  int nhash;
  size_t nrecs;
  size_t size;
  struct bucket *table;

  // stats
  int nhits;
  int ninserts;
  int nlookups_local;
  int nupdates_local;
  int naborts_local;
  int nlookups_remote;
  int nupdates_remote;
  int naborts_remote;

  struct txn_ctx txn_ctx;
  unsigned int seed;
  uint64_t q_idx;

#if SHARED_EVERYTHING
  /* each partition is assoc with a thread. In se case, some partitions 
   * might not have any data. None the less, for the time being, we can
   * use partition structure to keep thread-local state
   */
  sethread_state_t se_ready;
#elif SHARED_NOTHING
  LATCH_T latch;
#endif

  uint64_t tps;

  uint64_t busyclock;
  uint64_t idleclock;

  // partition-local heap
  struct mem_heap heap;

} __attribute__ ((aligned (CACHELINE)));

/**
 * Server/Client Message Passing Data Structures
 */
struct onewaybuffer {
  volatile uint64_t data[ONEWAY_BUFFER_SIZE];
  volatile unsigned long rd_index;
  volatile char padding0[CACHELINE - sizeof(long)];
  volatile unsigned long wr_index;
  volatile char padding1[CACHELINE - sizeof(long)];
  volatile unsigned long tmp_wr_index;
  volatile char padding2[CACHELINE - sizeof(long)];
} __attribute__ ((aligned (CACHELINE)));

struct box {
  struct onewaybuffer in;
  struct onewaybuffer out;
}  __attribute__ ((aligned (CACHELINE)));

struct box_array {
  struct box *boxes;
} __attribute__ ((aligned (CACHELINE)));

struct thread_args {
  int id;
  int core;
  struct hash_table *hash_table;
};

/*
 * Hash Table data structure
 */
struct hash_table {
  int nservers;
  volatile int nclients;
  size_t nrecs;

  pthread_mutex_t create_client_lock;

  volatile int quitting;
  pthread_t *threads;
  struct thread_args *thread_data;

  struct partition *partitions;
  struct partition *g_partition; /* used for item table */
  struct box_array *boxes;
  uint64_t *keys;

  // stats
  int track_cpu_usage;
};

#endif

