#include "headers.h"
#include "smphashtable.h"
#include "benchmark.h"
#include "twopl.h"
#include "plmalloc.h"

#if 0
enum lock_t {LOCK_EXCL, LOCK_SHRD, LOCK_NONE};

struct lock_item {
  lock_t type;

  TAILQ_ENTRY(lock_item) next_owner;
  TAILQ_ENTRY(lock_item) next_waiter;
};
#endif

#if SHARED_EVERYTHING


#if ANDERSON_LOCK
#pragma message ("Using ALOCK")
#elif PTHREAD_SPINLOCK
#pragma message ("Using pthread spinlock")
#else
#pragma message ("Using pthread mutex")
#endif

extern struct hash_table *hash_table;
extern struct benchmark *g_benchmark;
extern int write_threshold;

#if ENABLE_WAIT_DIE_CC
int selock_wait_die_acquire(struct partition *p, struct elem *e, 
    char optype, uint64_t req_ts)
{
  struct lock_entry *target, *l;
  int s = p - &hash_table->partitions[0];

  dprint("srv(%d-%"PRIu64"): %s lock request for key %"PRIu64"\n", s, req_ts,
      optype == OPTYPE_LOOKUP ? "lookup":"update", e->key);

  /* latch the record. check to see if it is a conflicting lock mode
   * if so, bad luck. we just fail
   */
  int r = 0;
  char wait = 0;
  int alock_state;

#if SE_LATCH
  LATCH_ACQUIRE(&e->latch, &alock_state);

  // there is a conflict if e's refcount indicates an exclusive lock
  char conflict = !is_value_ready(e);
  
#if VERIFY_CONSISTENCY
  //if conflict, there must be one and exactly one owner
  if (conflict) {
    int nowners = 0;
    TAILQ_FOREACH(l, &e->owners, next) {
      nowners++;
    }

    assert(nowners == 1);
  }
#endif

  // even if no exclusive lock, conflict if we want an update when
  // there are read locks
  if (!conflict && optype == OPTYPE_UPDATE)
    conflict = e->ref_count > 1;

  /* with wait die, we also have a conflict if there is a waiting list
   * and if head of waiting list is a newer txn than incoming one
   */
  if (!conflict) {
    if (!TAILQ_EMPTY(&e->waiters)) {
      l = TAILQ_FIRST(&e->waiters);
      if (req_ts <= l->ts)
        conflict = 1;
    }
  }

  /* if there are no conflicts, we reset refcount */
  if (!conflict) {
    dprint("srv(%d-%"PRIu64"): %s lock request for key %"PRIu64" granted w/o conflict\n", 
        s, req_ts, optype == OPTYPE_LOOKUP ? "lookup":"update", e->key);

    if (optype == OPTYPE_LOOKUP) {
   	  e->ref_count++;
      r = 1;
    } else {
      e->ref_count = DATA_READY_MASK | 2;
      r = 1;
    }

    // insert a lock in the owners list
    target = plmalloc_alloc(p, sizeof(struct lock_entry));
    assert(target);
    target->ts = req_ts;
    target->s = s;
    target->optype = optype;
    target->ready = 1;

    TAILQ_INSERT_HEAD(&e->owners, target, next);

  } else {

    /* There was a conflict. In wait die case, we can wait if req_ts is < ts 
     * of all owner txns 
     */
    wait = 1;
    TAILQ_FOREACH(l, &e->owners, next) {
      if (l->ts < req_ts || ((l->ts == req_ts) && (l->s < s))) {
        wait = 0;
        break;
      }
    }

    if (wait) {
      dprint("srv(%d-%"PRIu64"): %s lock request for key %"PRIu64" put under wait\n", 
        s, req_ts, optype == OPTYPE_LOOKUP ? "lookup":"update", e->key);

      // if we are allowed to wait, make a new lock entry and add it to
      // waiters list
      TAILQ_FOREACH(l, &e->waiters, next) {
        if (l->ts < req_ts || (l->ts == req_ts && l->s < s))
          break;
      }

      target = plmalloc_alloc(p, sizeof(struct lock_entry));
      assert(target);
      target->ts = req_ts;
      target->s = s;
      target->optype = optype;
      target->ready = 0;
      if (l) {
        TAILQ_INSERT_BEFORE(l, target, next);
      } else {
        if (TAILQ_EMPTY(&e->waiters)) {
          TAILQ_INSERT_HEAD(&e->waiters, target, next);
        } else {
          TAILQ_INSERT_TAIL(&e->waiters, target, next);
        }
      }
    } else {
      dprint("srv(%d-%"PRIu64"): %s lock request for key %"PRIu64" aborting\n", 
        s, req_ts, optype == OPTYPE_LOOKUP ? "lookup":"update", e->key);
    }
  }

  LATCH_RELEASE(&e->latch, &alock_state);

  if (wait) {
    assert(r == 0);

    /* now spin until another thread signals us that we have the lock */
    assert(target);

    dprint("srv(%d-%"PRIu64"): %s lock request for key %"PRIu64" spinning until ready\n", 
      s, req_ts, optype == OPTYPE_LOOKUP ? "lookup":"update", e->key);

    while (!target->ready) ;
  
    dprint("srv(%d-%"PRIu64"): %s lock request for key %"PRIu64" stopped spinning\n", 
      s, req_ts, optype == OPTYPE_LOOKUP ? "lookup":"update", e->key);

    if (optype == OPTYPE_LOOKUP) {
      assert(e->ref_count > 1);
    } else {
      assert((e->ref_count & DATA_READY_MASK) && 
        ((e->ref_count & ~DATA_READY_MASK) == 2));
    }

    r = 1;
  }

#else
  assert(g_benchmark == &micro_bench && write_threshold == 100);
  r = 1;
#endif // SE_LATCH

  return r;

}

void selock_wait_die_release(struct partition *p, struct elem *e)
{
  /* latch, reset ref count to free the logical lock, unlatch */
  int alock_state;
  int s = p - &hash_table->partitions[0];

#if SE_LATCH
  LATCH_ACQUIRE(&e->latch, &alock_state);

  dprint("srv(%d): releasing lock request for key %"PRIu64"\n", s, e->key);

  /* go through the owners list and remove the txn */
  struct lock_entry *lock_entry;

  TAILQ_FOREACH(lock_entry, &e->owners, next) {
    if (lock_entry->s == s)
      break;
  }
  
  if (!lock_entry) {
    dprint("srv(%d): FAILED releasing lock request for key %"PRIu64"\n", s, e->key);
  }

  assert(lock_entry);

  TAILQ_REMOVE(&e->owners, lock_entry, next);

  // decrement ref count as a owner is releasing a lock
  e->ref_count = (e->ref_count & ~DATA_READY_MASK) - 1;

  plmalloc_free(p, lock_entry, sizeof(*lock_entry));

  // if there are no more owners, then refcount should be 1
  if (TAILQ_EMPTY(&e->owners))
    assert(e->ref_count == 1);

   /* If lock_free is set, that means the new lock mode is decided by 
   * the head of waiter list. If lock_free is not set, we still have
   * some readers. So only pending readers can be allowed. Keep 
   * popping items from wait list as long as we have readers.
   */
  lock_entry = TAILQ_FIRST(&e->waiters);
  while (lock_entry) {
    char conflict = 0;

    dprint("srv(%d): release request for key %"PRIu64" found %d waiting\n", 
      s, e->key, lock_entry->s);

    if (lock_entry->optype == OPTYPE_LOOKUP) {
      conflict = !is_value_ready(e);
    } else {
      conflict = (!is_value_ready(e)) || ((e->ref_count & ~DATA_READY_MASK) > 1);
    }

    if (conflict) {
      dprint("srv(%d): release request for key %"PRIu64" found %d in conflict"
          "ref count was %"PRIu64"\n", s, e->key, lock_entry->s, e->ref_count);
      break;

    } else {
      /* there's no conflict only if there is a shared lock and we're 
       * requesting a shared lock, or if there's no lock
       */
      assert((e->ref_count & DATA_READY_MASK) == 0);

      if (lock_entry->optype == OPTYPE_LOOKUP) {
        assert((e->ref_count & ~DATA_READY_MASK) >= 1);
        e->ref_count++;
      } else {
        assert((e->ref_count & ~DATA_READY_MASK) == 1);
        e->ref_count = DATA_READY_MASK | 2;
      }
    }

    // remove from waiters, add to owners, mark as ready
    TAILQ_REMOVE(&e->waiters, lock_entry, next);
    TAILQ_INSERT_HEAD(&e->owners, lock_entry, next);
    lock_entry->ready = 1;

    dprint("srv(%d): release lock request for key %"PRIu64" marking %d as ready\n", 
      s, e->key, lock_entry->s);

    // go to next element
    lock_entry = TAILQ_FIRST(&e->waiters);
  }

  LATCH_RELEASE(&e->latch, &alock_state);

#else
  assert(g_benchmark == &micro_bench && write_threshold == 100);
#endif // SE_LATCH

}
#endif // ENABLE_WAIT_DIE_CC

int selock_nowait_acquire(struct partition *p, struct elem *e, char optype, 
    uint64_t req_ts)
{
  /* latch the record. check to see if it is a conflicting lock mode
   * if so, bad luck. we just fail
   */
  int r = 0;
  int alock_state;

#if SE_LATCH
  LATCH_ACQUIRE(&e->latch, &alock_state);

  // there is a conflict if e's refcount indicates an exclusive lock
  char conflict = !is_value_ready(e);

  // even if no exclusive lock, conflict if we want an update when
  // there are read locks
  if (!conflict && optype == OPTYPE_UPDATE)
    conflict = e->ref_count > 1;

  /* if there are no conflicts  locks, we reset refcount */
  if (!conflict) {
    if (optype == OPTYPE_LOOKUP) {
   	  e->ref_count++;
      r = 1;
    } else {
      e->ref_count = DATA_READY_MASK | 2;
      r = 1;
    }
  }

  LATCH_RELEASE(&e->latch, &alock_state);
#else
  assert(g_benchmark == &micro_bench && write_threshold == 100);
  r = 1;
#endif

  return r;
}

void selock_nowait_release(struct partition *p, struct elem *e)
{
  /* latch, reset ref count to free the logical lock, unlatch */
  int alock_state;

#if SE_LATCH
  LATCH_ACQUIRE(&e->latch, &alock_state);

  e->ref_count = (e->ref_count & (~DATA_READY_MASK)) - 1;

  LATCH_RELEASE(&e->latch, &alock_state);

#else
  assert(g_benchmark == &micro_bench && write_threshold == 100);
#endif
}

int selock_acquire(struct partition *p, struct elem *e, 
    char optype, uint64_t req_ts)
{
#if ENABLE_WAIT_DIE_CC
  return selock_wait_die_acquire(p, e, optype, req_ts);
#else
  return selock_nowait_acquire(p, e, optype, req_ts);
#endif
}

void selock_release(struct partition *p, struct elem *e)
{
#if ENABLE_WAIT_DIE_CC
  return selock_wait_die_release(p, e);
#else
  return selock_nowait_release(p, e);
#endif
}

#endif
