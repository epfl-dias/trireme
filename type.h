#ifndef __TYPE_H__
#define __TYPE_H__

#include <sys/queue.h>
#include <inttypes.h>
#include <stdbool.h>
#include <ucontext.h>

#include "const.h"
#include "hashprotocol.h"

/* lock used to implement 2pl wait die */
struct lock_entry {
  short task_id;
  short op_id;
  int s;
  uint64_t ts;
  char optype;
  volatile char ready;
#if ENABLE_DL_DETECT_CC
  int *notify;
  TAILQ_ENTRY(lock_entry) next;
#else
  LIST_ENTRY(lock_entry) next;
#endif
};

#if ENABLE_DL_DETECT_CC
TAILQ_HEAD(lock_tail, lock_entry);
#else
LIST_HEAD(lock_list, lock_entry);
#endif

/* dl_detect structures */
LIST_HEAD(adj_list, adj_list_entry);
struct adj_list_entry {
	uint64_t entry;
	LIST_ENTRY(adj_list_entry) next;
};

LIST_HEAD(int_pair_list, int_pair_list_entry);
struct int_pair_list_entry {
	uint64_t sender_srv;
	uint64_t cand_srv;
	uint64_t cand_fib;
	uint64_t cand_ts;
	struct elem *record_addr;
	int opid;
	LIST_ENTRY(int_pair_list_entry) next_int_pair;
};

// The denpendency information per thread
typedef struct {
	struct adj_list adj;
	size_t adj_size;
	pthread_mutex_t lock;
	volatile int64_t txnid; 				// -1 means invalid
	int num_locks;				// the # of locks that txn is currently holding
	char pad[2 * CL_SIZE - sizeof(int64_t) - sizeof(pthread_mutex_t) - sizeof(struct adj_list) - sizeof(int)];
} DepThd;

// shared data for a particular deadlock detection
typedef struct {
	bool * visited;
	bool * recStack;
	bool loop;
	bool onloop;		// the current node is on the loop
	int loopstart;		// the starting point of the loop
	int min_lock_num; 	// the min lock num for txn in the loop
	uint64_t min_txnid; // the txnid that holds the min lock num
} DetectData;

typedef struct {
	int V;    // No. of vertices
	DepThd * dependency;

	///////////////////////////////////////////
	// For deadlock detection
	///////////////////////////////////////////
	// dl_lock is the global lock. Only used when deadlock detection happens
	pthread_mutex_t _lock;
} DL_detect;

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

#elif RW_LOCK

#if CUSTOM_RW_LOCK

typedef struct
{
    unsigned int spinlock;
    unsigned readers;
} rwlock_t;

#define LATCH_T rwlock_t
#define LATCH_INIT(latch, nservers) rwlock_init(latch)
#define LATCH_ACQUIRE {assert(0)};
#define LATCH_RELEASE {assert(0)};

#else

typedef pthread_rwlock_t rwlock_t;

#define LATCH_T pthread_rwlock_t
#define LATCH_INIT(latch, nservers) pthread_rwlock_init(latch, NULL)
#define LATCH_ACQUIRE {assert(0)};
#define LATCH_RELEASE {assert(0)};
#define rwlock_rdtrylock(latch) !(pthread_rwlock_tryrdlock(latch));
#define rwlock_wrtrylock(latch) !(pthread_rwlock_trywrlock(latch));
#define rwlock_wrunlock(latch) pthread_rwlock_unlock(latch)
#define rwlock_rdunlock(latch) pthread_rwlock_unlock(latch)

#endif //CUSTOM_RW_LOCK

#elif DRW_LOCK

typedef struct
{
    struct {
        unsigned int spinlock;
        char pad[CACHELINE - 4];
    } latch[NCORES];
} drwlock_t;

#define LATCH_T drwlock_t
#define LATCH_INIT(latch, nservers) drwlock_init(latch)
#define LATCH_ACQUIRE {assert(0)};
#define LATCH_RELEASE {assert(0)};

#elif RWTICKET_LOCK
typedef union
{
  uint64_t u;
  struct {
    uint32_t wr;
  } i;
  struct {
    uint16_t writers;
    uint16_t readers;
    uint16_t next;
    uint16_t pad;
  } s;
}rwticket_t;

#define LATCH_T rwticket_t
#define LATCH_INIT(latch, nservers) rwticket_init(latch)
#define LATCH_ACQUIRE
#define LATCH_RELEASE

#elif TICKET_LOCK

typedef struct
{
  volatile uint32_t ticket;
  //char padding[CACHELINE - 4];
  volatile uint32_t users;
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

#elif SIMPLE_SPINLOCK

typedef unsigned int sspinlock_t;

#define LATCH_T sspinlock_t
#define LATCH_INIT(latch, nservers) sspinlock_init(latch)
#define LATCH_ACQUIRE(latch, state) sspinlock_acquire(latch)
#define LATCH_RELEASE(latch, state) sspinlock_release(latch)

#elif PTHREAD_SPINLOCK

#define LATCH_T pthread_spinlock_t
#define LATCH_INIT(latch, nservers) pthread_spin_init(latch, 0)
#define LATCH_ACQUIRE(latch, state) pthread_spin_lock(latch)
#define LATCH_TRY_ACQUIRE(latch) pthread_spin_trylock(latch)
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

#elif CLH_LOCK

typedef struct
{
    volatile uint8_t locked;
} clh_qnode;

typedef volatile clh_qnode *clh_qnode_ptr;
typedef clh_qnode_ptr clh_lock;

#elif NOLATCH

#define LATCH_T void*
#define LATCH_INIT(latch, nservers)
#define LATCH_ACQUIRE(latch, state)
#define LATCH_TRY_ACQUIRE(latch)
#define LATCH_RELEASE(latch, state)

#else
#define LATCH_T pthread_mutex_t
#define LATCH_INIT(latch, nservers) pthread_mutex_init(latch, NULL)
#define LATCH_ACQUIRE(latch, state) pthread_mutex_lock(latch)
#define LATCH_RELEASE(latch, state)  pthread_mutex_unlock(latch)
#endif

// TODO: This should the ts for all cases. Right now, its used only for mvcc
typedef struct {
    uint64_t tsval;
    int core;
} timestamp;
#define TSZERO {0,-1}

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
#if SHARED_EVERYTHING
#if CLH_LOCK
  clh_lock *lock;
#else
  LATCH_T latch;
#endif // CLH_LOCK
#endif

  // tid used for silo
#if ENABLE_SILO_CC
  uint64_t tid;
#endif

  // waiters and owners used for 2pl
#if ENABLE_DL_DETECT_CC
  struct lock_tail waiters;
  struct lock_tail owners;
#else
  struct lock_list waiters;
  struct lock_list owners;
#endif

#if ENABLE_MVTO
  // timestamps used for mvto
  timestamp ts;
  timestamp max_rd_ts;

  // mvcc also needs to maintain queue of versions
  TAILQ_HEAD(version_list, elem) versions;
  TAILQ_ENTRY(elem) prev_version;
#endif

#if ENABLE_MV2PL
  volatile int64_t rd_counter;
  volatile int64_t is_write_locked;
#endif

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
#if defined(SE_LATCH) && defined(SE_INDEX_LATCH)
  LATCH_T latch;
#endif
  struct elist chain;
}__attribute__ ((aligned (CACHELINE)));

struct op_ctx {
  int target;
  char optype; 
  struct elem *e;
  struct elem *data_copy;
  uint64_t tid_copy;
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
  TASK_STATE_READY, TASK_STATE_WAITING, TASK_STATE_FINISH, TASK_STATE_MIGRATE
} task_state;

struct task {
  short tid; // this is the tid local to each core
  short g_tid; // this tid will be global across all cores
  task_state state;
  ucontext_t ctx;
  int target;
  int origin;
  int s;
  struct txn_ctx txn_ctx;
  struct hash_query queries[NQUERIES_PER_TASK];
  short qidx;
  short npending;
  uint64_t received_responses[MAX_OPS_PER_QUERY];
  short nresponses;
#if GATHER_STATS
  int times_scheduled;
  double run_time;
  int naborts;
  int ncommits;
#endif
  TAILQ_ENTRY(task) next;
} __attribute__ ((aligned (CACHELINE)));

struct partition {
  struct bucket *table;
  uint64_t q_idx;
  int nhash;
  size_t nrecs;

#if SHARED_NOTHING
  LATCH_T latch;
#endif

#if CLH_LOCK
  clh_qnode *my_qnode;
  clh_qnode *my_pred;
#endif

  // partition-local heap
  struct mem_heap heap;

#if ENABLE_SILO_CC
  uint64_t cur_tid;
#endif

  unsigned int seed;

  // tasks
  struct task unblock_task;
  struct task root_task;
  ucontext_t main_ctx;
  struct task *current_task;

  TAILQ_HEAD(ready_list, task) ready_list; 
  TAILQ_HEAD(wait_list, task) wait_list; 
  TAILQ_HEAD(free_list, task) free_list; 

  // stats
  size_t size;
  int ninserts;
  int ncommits;
  int ncommits_ronly;
  int ncommits_wonly;
  int nlookups_local;
  int nupdates_local;
  int naborts_ronly;
  int naborts_wonly;
  int naborts;
  int nlookups_remote;
  int nupdates_remote;
  int nvalidate_success;
  int nvalidate_failure;

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
  struct partition *partitions;
  struct partition *g_partition; /* used for item table */

  struct box_array *boxes;
  uint64_t *keys;

  volatile int quitting;

  pthread_t *threads;
  struct thread_args *thread_data;
  pthread_mutex_t create_client_lock;
};

#endif

