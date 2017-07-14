#include "headers.h"
#include "smphashtable.h"
#include "plmalloc.h"

#if ENABLE_MVTO

struct elem *mvto_acquire(struct partition *p, struct elem *e,
    char optype, uint64_t tsval)
{
  struct elem *target = NULL;
  int s = p - &hash_table->partitions[0];
  int g_tid = p->current_task->g_tid;
  struct lock_entry *wait_lock = NULL;

  dprint("srv(%d-%"PRIu64"): %s lock request for key %"PRIu64"\n", s, tsval,
      optype == OPTYPE_LOOKUP ? "lookup":"update", e->key);

  char wait = 0;
  int alock_state;

#if !defined(SE_LATCH)
  assert(0);
#endif

  LATCH_ACQUIRE(&e->latch, &alock_state);

  timestamp oldest_ts = TSZERO;
  timestamp latest_ts = TSZERO;
  timestamp req_ts = {tsval, s};

  if (!TAILQ_EMPTY(&e->versions))
      oldest_ts = TAILQ_FIRST(&e->versions)->ts;

  if (!TAILQ_EMPTY(&e->versions))
      latest_ts = TAILQ_LAST(&e->versions, version_list)->ts;

  if (optype == OPTYPE_LOOKUP) {
      if (tscompare(&req_ts, &oldest_ts) == -1) {
          assert(0);
      } else if (tscompare(&req_ts, &latest_ts) == 1) {
          struct lock_entry *owner = LIST_FIRST(&e->owners);
          if (owner) {
              timestamp owner_ts = {owner->ts, owner->s};

              // we have to wait for current writer to finish
              // XXX: TODO
              if (tscompare(&owner_ts, &req_ts) == -1)
                  assert(0);
          } else {
              //reader is free to go ahead and read
              if (tscompare(&req_ts, &e->max_rd_ts) == 1)
                  e->max_rd_ts = req_ts;

              target = e;
          }
      } else {
          /* we have a read such that old_ts < req_ts < latest_ts
           * Find the right version now
           */
          struct elem *v;
          timestamp pts = TSZERO;
          TAILQ_FOREACH(v, &e->versions, prev_version) {
              if (tscompare(&v->ts, &req_ts) == 1)
                  break;

              // just a quick check to make sure ts is in asc. order
              assert (tscompare(&pts, &v->ts) == -1);
              pts = v->ts;
          }

          if (v) {
              // we were able to find an old version. we need to use that
              target = v;
          } else {
              // no old version matched. just use latest version
              target = e;
          }
      }
  } else {
      // if timestamps are off, abort
      if (tscompare(&req_ts, &latest_ts) == -1 ||
              tscompare(&req_ts, &e->max_rd_ts) == -1) {
          LATCH_RELEASE(&e->latch, &alock_state);
          return NULL;
      }

      struct lock_entry *owner = LIST_FIRST(&e->owners);
      if (owner) {
          // if someone is writing, and their ts is > us, abort
          timestamp owner_ts = {owner->ts, owner->s};
          if (tscompare(&owner_ts, &req_ts) == 1) {
              LATCH_RELEASE(&e->latch, &alock_state);
              return NULL;
          }

          // we need to wait
          wait = 1;
      } else {
          // writer can proceed
          target = e;

          // insert a lock in the owners list
          struct lock_entry *owner_lock =
              plmalloc_alloc(p, sizeof(struct lock_entry));

          assert(owner_lock);
          owner_lock->ts = req_ts.tsval;
          owner_lock->s = s;
          owner_lock->task_id = g_tid;
          owner_lock->optype = optype;
          owner_lock->ready = 1;

          LIST_INSERT_HEAD(&e->owners, owner_lock, next);
      }
  }

  if (wait) {
      struct lock_entry *last_lock = NULL;
      struct lock_entry *l;
      LIST_FOREACH(l, &e->waiters, next) {
          timestamp tmp_ts = {l->ts, l->s};
          if (tscompare(&tmp_ts, &req_ts) == 1)
              break;

          last_lock = l;
      }

      wait_lock = plmalloc_alloc(p, sizeof(struct lock_entry));
      assert(wait_lock);
      wait_lock->ts = tsval;
      wait_lock->s = s;
      wait_lock->task_id = g_tid;
      wait_lock->optype = optype;
      wait_lock->ready = 0;
      if (l) {
          LIST_INSERT_BEFORE(l, wait_lock, next);
      } else {
          if (last_lock) {
              LIST_INSERT_AFTER(last_lock, wait_lock, next);
          } else {
              LIST_INSERT_HEAD(&e->waiters, wait_lock, next);
          }
      }
  }

  LATCH_RELEASE(&e->latch, &alock_state);

  if (wait) {
      assert(wait_lock);

      while (!wait_lock->ready) ;

      target = e;
  }

  return target;
}

void mvto_release(struct partition *p, struct elem *e_new, struct elem *e_old)
{
  /* latch, reset ref count to free the logical lock, unlatch */
  int alock_state;
  int s = p - &hash_table->partitions[0];
  int g_tid = p->current_task->g_tid;

  LATCH_ACQUIRE(&e_new->latch, &alock_state);

  /* go through the owners list and remove the txn */
  struct lock_entry *l = LIST_FIRST(&e_new->owners);
  assert (l); 
  assert(l->task_id == g_tid);

  /* set the new timestamp in the record based on info in the lock entry */
  e_new->ts.tsval = l->ts;
  e_new->ts.core = l->s;

  LIST_REMOVE(l, next);
  plmalloc_free(p, l, sizeof(*l));

  assert(LIST_EMPTY(&e_new->owners));

  // link the old element version to the current element. e_old NULL means we
  // are aborting the transaction in which cases, there is nothing to do
  if (e_old) {
      TAILQ_INSERT_TAIL(&e_new->versions, e_old, prev_version);
      assert(TAILQ_LAST(&e_new->versions, version_list));
  }

  // now go through waiter list and pass on the lock to someone else
  // TODO: Fow now, we just make next writer lock owner. Thiis logic needs to
  // change when we support true read/write
  l = LIST_FIRST(&e_new->waiters);
  if (l) {
    LIST_REMOVE(l, next);
    LIST_INSERT_HEAD(&e_new->owners, l, next);
    l->ready = 1;
  }

  LATCH_RELEASE(&e_new->latch, &alock_state);
}

#endif //ENABLE_MVTO
