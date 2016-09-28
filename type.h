#ifndef __TYPE_H__
#define __TYPE_H__

#include <sys/queue.h>
#include <inttypes.h>

/* lock used to implement 2pl wait die */
struct lock_entry {
  short task_id;
  short op_id;
  int s;
  uint64_t ts;
  char optype;
  volatile char ready;
  LIST_ENTRY(lock_entry) next;
};

LIST_HEAD(lock_list, lock_entry);

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

#elif TAS_LOCK

typedef unsigned int taslock_t;
#define LATCH_T taslock_t
#define LATCH_INIT(latch, nservers) taslock_init(latch)
#define LATCH_ACQUIRE(latch, state) taslock_acquire(latch)
#define LATCH_RELEASE(latch, state) taslock_release(latch)

#elif PTHREAD_SPINLOCK
#define LATCH_T pthread_spinlock_t
#define LATCH_INIT(latch, nservers) pthread_spin_init(latch, 0)
#define LATCH_ACQUIRE(latch, state) pthread_spin_lock(latch)
#define LATCH_RELEASE(latch, state)  pthread_spin_unlock(latch)

#elif HTLOCK
typedef struct htlock_global
{
    volatile uint32_t nxt;
    volatile uint32_t cur;
    uint8_t padding[CACHELINE - 8];
} htlock_global_t;

typedef struct htlock_local
{
    volatile int32_t nxt;
    volatile int32_t cur;
    uint8_t padding[CACHELINE - 8];
} htlock_local_t;

typedef struct __attribute__ ((aligned (CACHELINE))) htlock
{
    htlock_global_t* global;
    htlock_local_t* local[NUMBER_OF_SOCKETS];
} htlock_t;

#define LATCH_T htlock_t
#define LATCH_INIT(latch, nservers) {create_htlock(latch); init_htlock(latch);}
#define LATCH_ACQUIRE(latch, state) htlock_lock(latch)
#define LATCH_RELEASE(latch, state)  htlock_release(latch)

#else
#define LATCH_T pthread_mutex_t
#define LATCH_INIT(latch, nservers) pthread_mutex_init(latch, NULL)
#define LATCH_ACQUIRE(latch, state) pthread_mutex_lock(latch)
#define LATCH_RELEASE(latch, state)  pthread_mutex_unlock(latch)
#endif

/* base elem used to represent a tuple */
struct elem {
  /* The total size in one cacheline including the latch and locks!!
   * This is based on the assumption that the latch will be a ptspinlock
   * which is only 4 bytes
   */
  hash_key key;
  unsigned int size;
  uint64_t ref_count;
  LIST_ENTRY(elem) chain;
  char *value;
  //uint64_t local_values[2];
#if SHARED_EVERYTHING
  LATCH_T latch;
#endif
  struct lock_list waiters; 
  struct lock_list owners;
} __attribute__ ((aligned (CACHELINE)));

LIST_HEAD(elist, elem);

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
  int target;
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

/* task/fiber types */
typedef enum {
  TASK_STATE_READY, TASK_STATE_WAITING, TASK_STATE_FINISH
} task_state;

struct task {
  int tid;
  task_state state;
  ucontext_t ctx;
  int s;
  struct txn_ctx txn_ctx;
  struct hash_query queries[NQUERIES_PER_TASK];
  short qidx;
  short npending;
  uint64_t received_responses[MAX_OPS_PER_QUERY];
  short nresponses;
  TAILQ_ENTRY(task) next;
} __attribute__ ((aligned (CACHELINE)));

struct partition {
  int nservers;
  int nhash;
  size_t nrecs;
  size_t size;
  struct bucket *table;

  // tasks
  struct task unblock_task;
  struct task root_task;
  ucontext_t main_ctx;
  struct task *current_task;

  TAILQ_HEAD(ready_list, task) ready_list; 
  TAILQ_HEAD(wait_list, task) wait_list; 
  TAILQ_HEAD(free_list, task) free_list; 

  // stats
  int ninserts;
  int ncommits;
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

