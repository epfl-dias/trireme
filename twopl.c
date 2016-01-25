#include "headers.h"
#include "smphashtable.h"
#include "plmalloc.h"

extern int write_threshold;

#if ENABLE_WAIT_DIE_CC
int wait_die_acquire(int s, struct partition *p, // necessary dses
    int c, struct elem *e, char optype, uint64_t req_ts, // input
    struct lock_entry **pl) // output
{
  struct lock_entry *l;
  int r, conflict;

  *pl = NULL;

  /* If we are on the owner's list, we have the element, ref counts are all
   * set, just save it and move along 
   */
  TAILQ_FOREACH(l, &e->owners, next) {
    if (l->s == c)
      break;
  }

  if (l) {
    dprint("srv(%d): cl %d update %" PRIu64 " rc %" PRIu64 
        " found in owner\n", s, c, e->key, e->ref_count);

    *pl = l;

    return LOCK_SUCCESS;
  }

  dprint("srv(%d): cl %d update %" PRIu64 " rc %" PRIu64 "\n", s, 
      c, e->key, e->ref_count);

  if (optype == OPTYPE_LOOKUP) {
    conflict = !is_value_ready(e);
  } else {
    assert(optype == OPTYPE_UPDATE);
    conflict = !is_value_ready(e) || e->ref_count > 1;
  }

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
 
  /* if somebody has lock or wait die is signaling a conflict
   * we need to check if this txn must wait or abort. If it must 
   * wait, then we should add it to waiters list and continue. 
   * SOmetime later, someone will move us to the owner list. At
   * that point, when we retry getting lock again, we will succeed.
   * So we will reply back only at that time. For now, we should
   * leave messages in queue and not remove them.
   *
   * If it must abort, then we should unlock all locks acquired,
   * then return back a NULL for each message.

   * XXX: We rely on state maintained in messaging system to ensure
   * that same ops are retried later. Given that we maintain an
   * explicit waiter list, another option would be to use async
   * messaging to send out a lock acquired response when the lock
   * is released. But doing this means that requests can get replied
   * to out of order. This is something messaging system cannot
   * handle now. 
   */
  if (conflict) {
    assert(write_threshold < 100);

    /* There was a conflict. In wait die case, we can wait if 
     * req_ts is < ts of all owner txns 
     */
    int wait = 1;
    TAILQ_FOREACH(l, &e->owners, next) {
      if (l->ts < req_ts || ((l->ts == req_ts) && (l->s < c))) {
        wait = 0;
        break;
      }
    }

    if (wait) {

      dprint("srv(%d): cl %d update %"PRIu64" rc %"PRIu64" waiting \n", 
          s, c, e->key, e->ref_count);

      // if we are allowed to wait, make a new lock entry and add it to
      // waiters list. Its possible that the request is being
      // retriedin which case it would already be on the waiters list
      int already_waiting = 0;
      TAILQ_FOREACH(l, &e->waiters, next) {
        // break if we're already waiting
        if (l->ts == req_ts && l->s == c) {
          assert(l->optype == OPTYPE_UPDATE);
          already_waiting = 1;
          *pl = l;
          break;
        }

        // break if this is where we have to insert ourself 
        if (l->ts < req_ts || (l->ts == req_ts && l->s < c))
          break;
      }

      if (!already_waiting) {
        struct lock_entry *target = plmalloc_alloc(p, sizeof(struct lock_entry));
        assert(target);
        target->ts = req_ts;
        target->s = c;
        target->optype = OPTYPE_UPDATE;
        target->ready = 0;
        *pl = target;

        if (l) {
          TAILQ_INSERT_BEFORE(l, target, next);
        } else {
          if (TAILQ_EMPTY(&e->waiters)) {
            TAILQ_INSERT_HEAD(&e->waiters, target, next);
          } else {
            TAILQ_INSERT_TAIL(&e->waiters, target, next);
          }
        }
      }

      r = LOCK_WAIT;
    } else {
      dprint("srv(%d): cl %d update %"PRIu64" rc %"PRIu64" aborted \n", 
          s, c, localbuf[k] & (~HASHOP_MASK), e[j]->ref_count);

      r = LOCK_ABORT;
    }
  } else {

    if (optype == OPTYPE_LOOKUP)
      e->ref_count++;
    else
      e->ref_count = DATA_READY_MASK | 2;

    // insert a lock in the owners list
    struct lock_entry *target = plmalloc_alloc(p, sizeof(struct lock_entry));
    assert(target);
    target->ts = req_ts;
    target->s = c;
    target->optype = OPTYPE_UPDATE;
    target->ready = 1;
    *pl = target;

    dprint("srv(%d): cl %d update %" PRIu64 " rc %" PRIu64 
        " adding to owners\n", s, c, e->key, e->ref_count);

    TAILQ_INSERT_HEAD(&e->owners, target, next);

    r = LOCK_SUCCESS;
  }

  return r;
}

void wait_die_release(int s, struct partition *p, int c, struct elem *e)
{
  // find out lock on the owners list
  struct lock_entry *l;
  TAILQ_FOREACH(l, &e->owners, next) {
    if (l->s == c)
      break;
  }

  if (l) {
    // free lock
    dprint("srv(%d): cl %d key %" PRIu64 " rc %" PRIu64 
        " being removed from owners\n", s, c, e->key, e->ref_count);

    TAILQ_REMOVE(&e->owners, l, next);

    mp_release_value_(p, e);
  } else {
    // it is possible that lock is on waiters list. Imagine a scenario
    // where we send requests in bulk to 2 servers, one waits and other
    // aborts. In this case, we will get release message for a request
    // that is currently waiting
    TAILQ_FOREACH(l, &e->waiters, next) {
      if (l->s == c)
        break;
    }

    // can't be on neither owners nor waiters!
    assert(l);
    TAILQ_REMOVE(&e->waiters, l, next);
  }

  plmalloc_free(p, l, sizeof(struct lock_entry));

  // if there are no more owners, then refcount should be 1
  if (TAILQ_EMPTY(&e->owners) && e->ref_count != 1) {
    printf("found key %"PRIu64" with ref count %d\n", e->key, e->ref_count);
    fflush(stdout);
    assert(0);
  }

  /* If lock_free is set, that means the new lock mode is decided by 
   * the head of waiter list. If lock_free is not set, we still have
   * some readers. So only pending readers can be allowed. Keep 
   * popping items from wait list as long as we have readers.
   */
  l = TAILQ_FIRST(&e->waiters);
  while (l) {
    char conflict = 0;

    if (l->optype == OPTYPE_LOOKUP) {
      conflict = !is_value_ready(e);
    } else {
      conflict = (!is_value_ready(e)) || ((e->ref_count & ~DATA_READY_MASK) > 1);
    }

    if (conflict) {
      break;
    } else {
      /* there's no conflict only if there is a shared lock and we're 
       * requesting a shared lock, or if there's no lock
       */
      assert((e->ref_count & DATA_READY_MASK) == 0);

      if (l->optype == OPTYPE_LOOKUP) {
        assert((e->ref_count & ~DATA_READY_MASK) >= 1);
        e->ref_count++;
      } else {
        assert((e->ref_count & ~DATA_READY_MASK) == 1);
        e->ref_count = DATA_READY_MASK | 2;
      }
    }

    // remove from waiters, add to owners, mark as ready
    TAILQ_REMOVE(&e->waiters, l, next);
    TAILQ_INSERT_HEAD(&e->owners, l, next);

    dprint("srv(%d): release lock request for key %"PRIu64" marking %d as ready\n", 
        s, e->key, l->s);

    // go to next element
    l = TAILQ_FIRST(&e->waiters);
  }
}
#endif

void no_wait_release(struct partition *p, struct elem *e)
{
  mp_release_value_(p, e);
}

int no_wait_acquire(struct elem *e, char optype)
{
  int r;

  dprint("srv(%d): cl %d update %" PRIu64 " rc %" PRIu64 "\n", s, 
      c, e->key, e->ref_count);

  if (optype == OPTYPE_LOOKUP) {
    if (!is_value_ready(e)) {
      r = LOCK_ABORT;
    } else {
      e->ref_count++;
      r = LOCK_SUCCESS;
    }
  } else {
    assert(optype == OPTYPE_UPDATE);
    if (!is_value_ready(e) || e->ref_count > 1) {
      r = LOCK_ABORT;
    } else {
      e->ref_count = DATA_READY_MASK | 2;
      r = LOCK_SUCCESS;
    }
  }

  return r;
}
 
