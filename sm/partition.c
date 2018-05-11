#include <math.h>

#include "headers.h"
#include "partition.h"
#include "benchmark.h"
#include "tpcc.h"
#include "plmalloc.h"

#define BUCKET_LOAD (2 * g_bench->get_avg_tuple_size())

#define SE_PRIME_MAGIC 100663319
#define SN_NMAGIC 8
int sn_prime_magic[] = {100663319, 50331653, 25165843, 12582917, 6291469,
    3145739, 1572869, 786433};

void init_hash_partition(struct partition *p, size_t nrecs, char alloc)
{
  int i;
  assert(nrecs != 0);
  assert((unsigned long)p % CACHELINE == 0);
  p->nrecs = nrecs;
  p->size = 0;

  /* XXX: For now, we set #buckets = #recs. This is done to reduce the shared
   * everything case's index latching overhead. Later, we need to implement a
   * proper hashtable.
   */
  if (g_benchmark == &ycsb_bench) {
      p->nhash = nrecs;
  } else {
#if SHARED_EVERYTHING
      p->nhash = SE_PRIME_MAGIC;
#else
      // size the HT buckets roughly halving size each time we cross a socket
      // XXX: This is very rough to keep HT size more or less constatn. 
      // Fix this later
      int ncores_per_socket = NCORES / NSOCKETS;
      int off = (g_nservers + ncores_per_socket -1) / ncores_per_socket  - 1;
      assert(off < SN_NMAGIC);

      p->nhash = sn_prime_magic[off];
#endif
  }

  printf("Server %d allocating %d buckets of size %d for HT\n",
          hash_table->partitions - p, p->nhash, sizeof(struct bucket));

  p->q_idx = 0;
  p->ninserts = 0;
  p->ncommits = 0;
  p->ncommits_ronly = 0;
  p->ncommits_wonly = 0;
  p->nlookups_local = 0;
  p->nupdates_local = 0;
  p->nvalidate_success = 0;
  p->nvalidate_failure = 0;
  p->naborts = 0;
  p->nlookups_remote = 0;
  p->nupdates_remote = 0;

  p->total_txn_latency = p->max_txn_latency = 0;
  p->min_txn_latency = UINT64_MAX;

  p->seed = rand();

#if SHARED_NOTHING
  LATCH_INIT(&p->latch, g_nservers);
#endif

#if CLH_LOCK
    p->my_qnode = (clh_qnode*) malloc(sizeof(clh_qnode));
    p->my_qnode->locked=0;
    p->my_pred = NULL;
    __sync_synchronize();
#endif

#if ENABLE_SILO_CC
    p->cur_tid = 0;
#endif

#if defined(ENABLE_SVDREADLOCK_CC) || defined(ENABLE_MVDREADLOCK_CC) || defined(ENABLE_FSVDREADLOCK_CC)
    p->waiting_for = -1;
#endif

  if (alloc) {
    p->table = memalign(CACHELINE, p->nhash * sizeof(struct bucket));
    assert((unsigned long) &(p->table[0]) % CACHELINE == 0);
    for (i = 0; i < p->nhash; i++) {
      LIST_INIT(&(p->table[i].chain));
#if defined(SE_LATCH) && defined(SE_INDEX_LATCH)
      LATCH_INIT(&p->table[i].latch, g_nservers);
#endif
    }

  }

  /* initialize partition-local heap */
  plmalloc_init(p);
}

size_t destroy_hash_partition(struct partition *p)
{
  int i;
  size_t dbg_p_size = 0;
  size_t act_p_size = p->size;

  for (i = 0; i < p->nhash; i++) {
    struct elist *eh = &p->table[i].chain;
    struct elem *e = LIST_FIRST(eh);
    while (e != NULL) {
      struct elem *next = LIST_NEXT(e, chain);
      dbg_p_size += sizeof(struct elem) + e->size;
      //release(e);
      hash_remove(p, e);
      e = next;
    }
  }

  /* free the heap */
  plmalloc_destroy(p);

#if SHARED_EVERYTHING
  /* in shared everything config, every thread uses its own partition
   * structure but all threads share the same bucket array.
   * so size computed from the bucket here will not match size of just
   * one partition if we have > 1 server
   */
#else
  assert(p->size == 0 && act_p_size == dbg_p_size);
#endif

  free(p->table);

  return dbg_p_size;
}

/**
 * hash_get_bucket: returns bucket were given key is or should be placed
 */
int hash_get_bucket(const struct partition *p, hash_key key)
{
  assert(p->nhash != 0);
  return key % p->nhash;
}

void hash_remove(struct partition *p, struct elem *e)
{
  int alock_state;
  int h = hash_get_bucket(p, e->key);
  struct bucket *b = &p->table[h];

#if defined(SE_LATCH) && defined(SE_INDEX_LATCH)
  LATCH_ACQUIRE(&b->latch, &alock_state);
#endif

  struct elist *eh = &b->chain;
  p->size -= (sizeof(struct elem) + e->size);
  assert(p->size >= 0);
  LIST_REMOVE(e, chain);

  //if (e->value != (char *)e->local_values) {
    //free(e->value);
    plmalloc_free(p, e->value, e->size);
  //}
  plmalloc_efree(p, e);

//  dprint("Deleted %"PRId64"\n", e->key);

#if defined(SE_LATCH) && defined(SE_INDEX_LATCH)
  LATCH_RELEASE(&b->latch, &alock_state);
#endif

}

struct elem * hash_lookup(struct partition *p, hash_key key)
{
  int alock_state;
  int h = hash_get_bucket(p, key);
  struct bucket *b = &p->table[h];

#if defined(SE_LATCH) && defined(SE_INDEX_LATCH)
  LATCH_ACQUIRE(&b->latch, &alock_state);
#endif

  struct elist *eh = &b->chain;
  struct elem *e = LIST_FIRST(eh);

  while (e != NULL) {
    if (e->key == key) {
      break;
    }

    e = LIST_NEXT(e, chain);
  }

#if defined(SE_LATCH) && defined(SE_INDEX_LATCH)
  LATCH_RELEASE(&b->latch, &alock_state);
#endif
  return e;
}

struct elem *hash_insert(struct partition *p, hash_key key, int size,
        release_value_f *release)
{
  int h = hash_get_bucket(p, key);

#if SHARED_EVERYTHING
  /* Quick hack to make all threads in SE load data. With the exception of ITEM
   * table which is loaded in global partition, rest of data is pointed to by
   * hashtable belonging to the first partition
   */
  struct bucket *b;
  if ((key & TID_MASK) == ITEM_TID) {
    b = &p->table[h];
  } else {
    b = &(hash_table->partitions[0].table[h]);
  }
#else
  struct bucket *b = &p->table[h];
#endif
  struct elem *e;
  int alock_state;

#if VERIFY_CONSISTENCY
  e = hash_lookup(p, key);
  assert (e == NULL || (key & ORDER_TID));
#endif

#if defined(SE_LATCH) && defined(SE_INDEX_LATCH)
  LATCH_ACQUIRE(&b->latch, &alock_state);
#endif

  struct elist *eh = &b->chain;

  // try to allocate space for new value
  //e = (struct elem *) memalign(CACHELINE, sizeof(struct elem));
  e = plmalloc_ealloc(p);
  assert (e);

#if SHARED_EVERYTHING
#if CLH_LOCK
  e->lock = (clh_lock *) malloc(sizeof(clh_lock));
  clh_qnode *sentinel = (clh_qnode *) malloc(sizeof(clh_qnode));
  sentinel->locked=0;
  *(e->lock) = sentinel;
  __sync_synchronize();

#else
  LATCH_INIT(&e->latch, g_nservers);
#endif // CLH_LOCK
#endif

  e->key = key;

  // if size fits locally, store locally. Else alloc
  //if (size <= sizeof(e->local_values)) {
  //  e->value = (char *)e->local_values;
  //} else {
    //e->value = malloc(size);
  e->value = plmalloc_alloc(p, size);
  //}
  assert(e->value);

  e->size = size;
  p->size += sizeof(struct elem) + size;

#if ENABLE_WAIT_DIE_CC
  LIST_INIT(&e->owners);
  LIST_INIT(&e->waiters);
#elif ENABLE_DL_DETECT_CC
  TAILQ_INIT(&e->owners);
  TAILQ_INIT(&e->waiters);
#endif

#if ENABLE_MVTO
  TAILQ_INIT(&e->versions);
  memset(&e->ts, 0, sizeof(timestamp));
  memset(&e->max_rd_ts, 0, sizeof(timestamp));
#elif ENABLE_MV2PL
  e->rd_counter = e->is_write_locked = 0;
#elif ENABLE_MV2PL_DRWLOCK
  for (int i = 0; i < NCORES; i++)
      e->rd_counter[i].value = 0;
  e->is_write_locked = 0;
#elif defined(ENABLE_SVDREADLOCK_CC)
  for (int i = 0; i < NCORES; i++)
      e->owners[i].spinlock = -1;
#elif defined(ENABLE_FSVDREADLOCK_CC)
  for (int i = 0; i < NCORES; i++) {
      e->owners[i].id = -1;
      e->waiters[i] = -1;
  }

  e->tlock.ticket = e->tlock.users = 0;

#elif defined(ENABLE_MVDREADLOCK_CC)
  for (int i = 0; i < NCORES; i++)
      e->owners[i].spinlock = -1;
  e->writer = -1;
#elif ENABLE_SILO_CC
  e->tid = 0;
#endif

  if (!LIST_EMPTY(eh)) {
      assert(g_benchmark == &tpcc_bench);
  }

  LIST_INSERT_HEAD(eh, e, chain);

#if defined(SE_LATCH) && defined(SE_INDEX_LATCH)
  LATCH_RELEASE(&b->latch, &alock_state);
#endif

  return e;
}
