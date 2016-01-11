#ifndef __TYPE_H__
#define __TYPE_H__

#include <sys/queue.h>
#include <inttypes.h>

/**
 * hash_key - Hash table key type
 */
typedef uint64_t hash_key;

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
  volatile unsigned int ticket;
  volatile unsigned int users;
} tlock_t;

#define LATCH_T tlock_t
#define LATCH_INIT(latch, nservers) tlock_init(latch)
#define LATCH_ACQUIRE(latch, state) tlock_acquire(latch)
#define LATCH_RELEASE(latch, state) tlock_unlock(latch)
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

/**
 * Hash Table Storage Data Structures
 * struct elem       - element in table
 * struct bucket     - a bucket in a partition
 * struct partition  - hash table partition for server
 */
struct elem {
  // size must be 64 bytes
  hash_key key;
  size_t size;
  uint64_t ref_count;
  TAILQ_ENTRY(elem) chain;
  char *value;
  uint64_t local_values[2];
#if SHARED_EVERYTHING
  LATCH_T latch;
#endif
} __attribute__ ((aligned (CACHELINE)));

TAILQ_HEAD(elist, elem);

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

  // stats
  int nhits;
  int ninserts;
  int nlookups_local;
  int nupdates_local;
  int naborts_local;

  int nlookups_remote;
  int nupdates_remote;
  int naborts_remote;

  uint64_t tps;

  uint64_t busyclock;
  uint64_t idleclock;

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

