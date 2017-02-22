#include "headers.h"
#include "smphashtable.h"
#include "benchmark.h"
#include "twopl.h"
#include "plmalloc.h"

#if SHARED_EVERYTHING

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
    LIST_FOREACH(l, &e->owners, next) {
      nowners++;
    }

    assert(nowners == 1);
  }
#endif

  // even if no exclusive lock, conflict if we want an update when
  // there are read locks
  if (!conflict && (optype == OPTYPE_UPDATE || optype == OPTYPE_INSERT))
    conflict = (e->ref_count > 1);

  /* with wait die, we also have a conflict if there is a waiting list
   * and if head of waiting list is a newer txn than incoming one
   */
  if (!conflict) {
    if (!LIST_EMPTY(&e->waiters)) {
      l = LIST_FIRST(&e->waiters);
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

    LIST_INSERT_HEAD(&e->owners, target, next);

  } else {

    /* There was a conflict. In wait die case, we can wait if req_ts is < ts 
     * of all owner txns 
     */
    wait = 1;
    LIST_FOREACH(l, &e->owners, next) {
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
      struct lock_entry *last_lock = NULL;
      LIST_FOREACH(l, &e->waiters, next) {
        if (l->ts < req_ts || (l->ts == req_ts && l->s < s))
          break;

        last_lock = l;
      }

      target = plmalloc_alloc(p, sizeof(struct lock_entry));
      assert(target);
      target->ts = req_ts;
      target->s = s;
      target->optype = optype;
      target->ready = 0;
      if (l) {
        LIST_INSERT_BEFORE(l, target, next);
      } else {
        if (last_lock) {
          LIST_INSERT_AFTER(last_lock, target, next);
        } else {
          LIST_INSERT_HEAD(&e->waiters, target, next);
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
  assert(g_benchmark == &micro_bench && g_write_threshold == 100);
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

  LIST_FOREACH(lock_entry, &e->owners, next) {
    if (lock_entry->s == s)
      break;
  }
  
  if (!lock_entry) {
    printf("srv(%d): FAILED releasing lock request for key %"PRIu64"\n", s, e->key);
  }

  assert(lock_entry);

  LIST_REMOVE(lock_entry, next);

  // decrement ref count as a owner is releasing a lock
  e->ref_count = (e->ref_count & ~DATA_READY_MASK) - 1;

  plmalloc_free(p, lock_entry, sizeof(*lock_entry));

  // if there are no more owners, then refcount should be 1
  if (LIST_EMPTY(&e->owners))
    assert(e->ref_count == 1);

   /* If lock_free is set, that means the new lock mode is decided by 
   * the head of waiter list. If lock_free is not set, we still have
   * some readers. So only pending readers can be allowed. Keep 
   * popping items from wait list as long as we have readers.
   */
  lock_entry = LIST_FIRST(&e->waiters);
  while (lock_entry) {
    char conflict = 0;

    dprint("srv(%d): release request for key %"PRIu64" found %d waiting\n", 
      s, e->key, lock_entry->s);

    if (lock_entry->optype == OPTYPE_LOOKUP) {
      conflict = !is_value_ready(e);
    } else {
      assert(lock_entry->optype == OPTYPE_UPDATE);
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
    LIST_REMOVE(lock_entry, next);
    LIST_INSERT_HEAD(&e->owners, lock_entry, next);
    lock_entry->ready = 1;

    dprint("srv(%d): release lock request for key %"PRIu64" marking %d as ready\n", 
      s, e->key, lock_entry->s);

    // go to next element
    lock_entry = LIST_FIRST(&e->waiters);
  }

  LATCH_RELEASE(&e->latch, &alock_state);

#else
  assert(g_benchmark == &micro_bench && g_write_threshold == 100);
#endif // SE_LATCH

}
#endif // ENABLE_WAIT_DIE_CC

#if ENABLE_NOWAIT_OWNER_CC
/*
 * NOWAIT implementation that maintains an owner list
 */
int selock_nowait_with_ownerlist_acquire(struct partition *p, struct elem *e,
        char optype, uint64_t req_ts)
{
  int r = 0, alock_state;

  LATCH_ACQUIRE(&e->latch, &alock_state);

  // there is a conflict if e's refcount indicates an exclusive lock
  char conflict = !is_value_ready(e);

  // even if no exclusive lock, conflict if we want an update when
  // there are read locks
  if (!conflict && (optype == OPTYPE_UPDATE || optype == OPTYPE_INSERT))
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

  if (r) {
      // success. add to owner list
      struct lock_entry *target  = plmalloc_alloc(p, sizeof(struct lock_entry));
      assert(target);
      target->s = p - hash_table->partitions; 
      target->optype = optype;
      target->ready = 1;

      LIST_INSERT_HEAD(&e->owners, target, next);
  }

  LATCH_RELEASE(&e->latch, &alock_state);

  return r;
}

void selock_nowait_with_ownerlist_release(struct partition *p, struct elem *e)
{
  struct lock_entry *lock_entry;
  int s = p - hash_table->partitions;
  int alock_state;

  LATCH_ACQUIRE(&e->latch, &alock_state);

  // remove from owner list
  LIST_FOREACH(lock_entry, &e->owners, next) {
      if (lock_entry->s == s)
          break;
  }
  
  assert(lock_entry);

  LIST_REMOVE(lock_entry, next);

  plmalloc_free(p, lock_entry, sizeof(*lock_entry));

  e->ref_count = (e->ref_count & (~DATA_READY_MASK)) - 1;

  LATCH_RELEASE(&e->latch, &alock_state);

}
#endif //ENABLE_NOWAIT_OWNER_CC

#if ENABLE_NOWAIT_CC
/*
 * NOWAIT implementation that does not maintain any lists explicitly
 */
int selock_nowait_acquire(struct partition *p, struct elem *e, char optype, 
    uint64_t req_ts)
{
  /* latch the record. check to see if it is a conflicting lock mode
   * if so, bad luck. we just fail
   */
  int r = 0;
  int alock_state;

#if SE_LATCH

#if RWTICKET_LOCK
  int nretries = 10;
  while (nretries-- && !r) {
    if (optype == OPTYPE_LOOKUP) {
      r = rwticket_rdtrylock(&e->latch);
    }  else {
      r = rwticket_wrtrylock(&e->latch);
    }
  }

#elif RW_LOCK
  if (optype == OPTYPE_LOOKUP) {
      r = rwlock_rdtrylock(&e->latch);
  }  else {
      r = rwlock_wrtrylock(&e->latch);
  }
#elif DRW_LOCK
  if (optype == OPTYPE_LOOKUP) {
      r = drwlock_rdtrylock(p - &(hash_table->partitions[0]), &e->latch);
  }  else {
      r = drwlock_wrtrylock(&e->latch);
  }

#else
  LATCH_ACQUIRE(&e->latch, &alock_state);

  // there is a conflict if e's refcount indicates an exclusive lock
  char conflict = !is_value_ready(e);

  // even if no exclusive lock, conflict if we want an update when
  // there are read locks
  if (!conflict && (optype == OPTYPE_UPDATE || optype == OPTYPE_INSERT))
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
#endif //RWTICKET_LOCK

#else
  // only time latching is disabled is when we are doing ronly bench
  assert(g_benchmark == &micro_bench && g_write_threshold == 100);
  r = 1;
#endif //SE_LATCH

  return r;
}

void selock_nowait_release(struct partition *p, struct elem *e)
{
  /* latch, reset ref count to free the logical lock, unlatch */
  int alock_state;

#if SE_LATCH
#if RWTICKET_LOCK
  /* XXX: We need to know the type of lock we are releasing here.
   * We can get that information from txn logic. But its a lot of work
   * refactoring the code to pass it through.
   *
   * For now, we assume all reads or all writes. 
   */
  assert(g_write_threshold == 0 || g_write_threshold == 100);

  if (g_write_threshold)
      rwticket_rdunlock(&e->latch);
  else
      rwticket_wrunlock(&e->latch);

#elif RW_LOCK
  /* XXX: We need to know the type of lock we are releasing here.
   * We can get that information from txn logic. But its a lot of work
   * refactoring the code to pass it through.
   *
   * For now, we assume all reads or all writes. 
   */
   assert(g_write_threshold == 0 || g_write_threshold == 100);

  if (g_write_threshold)
      rwlock_rdunlock(&e->latch);
  else
      rwlock_wrunlock(&e->latch);

#elif DRW_LOCK
  /* XXX: We need to know the type of lock we are releasing here.
   * We can get that information from txn logic. But its a lot of work
   * refactoring the code to pass it through.
   *
   * For now, we assume all reads or all writes. 
   */
   assert(g_write_threshold == 0 || g_write_threshold == 100);

  if (g_write_threshold)
      drwlock_rdunlock(p - &(hash_table->partitions[0]), &e->latch);
  else
      drwlock_wrunlock(&e->latch);

#else
  LATCH_ACQUIRE(&e->latch, &alock_state);

  e->ref_count = (e->ref_count & (~DATA_READY_MASK)) - 1;

  LATCH_RELEASE(&e->latch, &alock_state);
#endif // RWTICKET_LOCK
#endif //SE_LATCH

}
#endif//IF_ENABLE_NOWAIT

int selock_acquire(struct partition *p, struct elem *e, 
    char optype, uint64_t req_ts)
{
#if ENABLE_WAIT_DIE_CC
  return selock_wait_die_acquire(p, e, optype, req_ts);
#elif ENABLE_NOWAIT_OWNER_CC
  return selock_nowait_with_ownerlist_acquire(p, e, optype, req_ts);
#elif ENABLE_NOWAIT_CC
  return selock_nowait_acquire(p, e, optype, req_ts);
#elif ENABLE_SILO_CC
  // with silo, nothing to do
  return 1;
#else
#error "No CC algorithm specified"
#endif
}

void selock_release(struct partition *p, struct elem *e)
{
#if ENABLE_WAIT_DIE_CC
  return selock_wait_die_release(p, e);
#elif ENABLE_NOWAIT_OWNER_CC
  return selock_nowait_with_ownerlist_release(p, e);
#elif ENABLE_NOWAIT_CC
  return selock_nowait_release(p, e);
#elif ENABLE_SILO_CC
  // with silo, nothing to do
  return;
#else
#error "No CC algorithm specified"
#endif
}

#endif
