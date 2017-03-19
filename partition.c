#include <math.h>

#include "headers.h"
#include "partition.h"
#include "tpcc.h"
#include "plmalloc.h"

void init_hash_partition(struct partition *p, size_t nrecs, char alloc)
{
  int i;

  assert((unsigned long)p % CACHELINE == 0);
  p->nrecs = nrecs;
  p->size = 0;

  /* XXX: For now, we set #buckets = #recs. This is done to reduce the shared 
   * everything case's index latching overhead. Later, we need to implement a
   * proper hashtable.
   */
  p->nhash = nrecs;

  p->q_idx = 0;
  p->ninserts = 0;
  p->ncommits = 0;
  p->nlookups_local = 0;
  p->nupdates_local = 0;
  p->naborts_local = 0;

  p->nlookups_remote = 0;
  p->nupdates_remote = 0;
  p->naborts_remote = 0;

  p->busyclock = 0;
  p->idleclock = 0;
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
  struct bucket *b = &p->table[h];
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

  LIST_INSERT_HEAD(eh, e, chain);

#if defined(SE_LATCH) && defined(SE_INDEX_LATCH)
  LATCH_RELEASE(&b->latch, &alock_state); 
#endif

  return e;
}

