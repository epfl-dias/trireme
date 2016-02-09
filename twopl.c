#include "headers.h"
#include "smphashtable.h"
#include "plmalloc.h"

extern int write_threshold;

#if ENABLE_WAIT_DIE_CC
int wait_die_check_acquire(int s, struct partition *p,
    int c, int tid, int opid, struct elem *e, char optype, uint64_t req_ts)
{
  char conflict;
  int r;
  struct lock_entry *l;

  if (optype == OPTYPE_LOOKUP) {
    conflict = !is_value_ready(e);
  } else {
    assert(optype == OPTYPE_UPDATE);
    conflict = !is_value_ready(e) || e->ref_count > 1;
  }

  if (!conflict) {
    if (!LIST_EMPTY(&e->waiters)) {
      l = LIST_FIRST(&e->waiters);
      if (req_ts <= l->ts)
        conflict = 1;
    }
  }
 
  if (conflict) {
    /* There was a conflict. In wait die case, we can wait if 
     * req_ts is < ts of all owner txns 
     */
    int wait = 1;
    LIST_FOREACH(l, &e->owners, next) {
      if (l->ts < req_ts || ((l->ts == req_ts) && (l->s < c))) {
        wait = 0;
        break;
      }
    }

    if (wait) {
      r = LOCK_WAIT;
    } else {
      r = LOCK_ABORT;
    }
  } else {
    r = LOCK_SUCCESS;
  }

  return r;
}

int wait_die_acquire(int s, struct partition *p,
    int c, int task_id, int op_id, struct elem *e, char optype, 
    uint64_t req_ts, struct lock_entry **pl)
{
  struct lock_entry *l;
  int r, conflict;

  *pl = NULL;

#if VERIFY_CONSISTENCY
  /* we cannot be on the owners list */
  LIST_FOREACH(l, &e->owners, next) {
    if (l->s == c && l->task_id == task_id && l->op_id == op_id)
      assert(0);
  }

  /* we cannot be on the waiters list */
  LIST_FOREACH(l, &e->waiters, next) {
    if (l->s == c && l->task_id == task_id && l->op_id == op_id)
      assert(0);
  }
#endif

  dprint("srv(%d): cl %d update %" PRIu64 " rc %" PRIu64 "\n", s, 
      c, e->key, e->ref_count);

  if (optype == OPTYPE_LOOKUP) {
    conflict = !is_value_ready(e);
  } else {
    assert(optype == OPTYPE_UPDATE || optype == OPTYPE_INSERT);
    conflict = (!is_value_ready(e)) || (e->ref_count > 1);
  }

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
 
  /* if somebody has lock or wait die is signaling a conflict
   * we need to check if this txn must wait or abort. If it must 
   * wait, then we should add it to waiters list and continue. 
   * Sometime later, someone will move us to the owner list and reply back 
   * to the requestor
   *
   * If it must abort, then we should unlock all locks acquired,
   * then return back a NULL for each message.

  */
  if (conflict) {

    /* There was a conflict. In wait die case, we can wait if 
     * req_ts is < ts of all owner txns 
     */
    int wait = 1;
    LIST_FOREACH(l, &e->owners, next) {
      if (l->ts < req_ts || ((l->ts == req_ts) && (l->s < c)) || 
          ((l->ts == req_ts) && (l->s == c) && (l->task_id < task_id))) {
        wait = 0;
        break;
      }
    }

    if (wait) {

      dprint("srv(%d): cl %d update %"PRIu64" rc %"PRIu64" waiting \n", 
          s, c, e->key, e->ref_count);

      // if we are allowed to wait, make a new lock entry and add it to
      // waiters list. 
      struct lock_entry *last_lock = NULL;
      LIST_FOREACH(l, &e->waiters, next) {

        /* there cannot be a request already from same server/task/op combo */
        if (l->s == c && l->task_id == task_id && l->op_id == op_id)
          assert(0);

        // break if this is where we have to insert ourself 
        if (l->ts < req_ts || (l->ts == req_ts && l->s < c) ||
          ((l->ts == req_ts) && (l->s == c) && (l->task_id < task_id)))
          break;

        last_lock = l;
      }

      struct lock_entry *target = plmalloc_alloc(p, sizeof(struct lock_entry));
      assert(target);
      target->ts = req_ts;
      target->s = c;

      target->optype = optype;
      target->ready = 0;
      target->task_id = task_id;
      target->op_id = op_id;
      *pl = target;

      if (l) {
        LIST_INSERT_BEFORE(l, target, next);
      } else {
        if (last_lock) {
          LIST_INSERT_AFTER(last_lock, target, next);
        } else {
          LIST_INSERT_HEAD(&e->waiters, target, next);
        }
      }

      r = LOCK_WAIT;
    } else {
      dprint("srv(%d): cl %d update %"PRIu64" rc %"PRIu64" aborted \n", 
          s, c, e->key, e->ref_count);

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
    target->optype = optype;
    target->ready = 1;
    target->task_id = task_id;
    target->op_id = op_id;
    *pl = target;

    dprint("srv(%d): cl %d update %" PRIu64 " rc %" PRIu64 
        " adding to owners\n", s, c, e->key, e->ref_count);

    LIST_INSERT_HEAD(&e->owners, target, next);

    r = LOCK_SUCCESS;
  }

  return r;
}

void wait_die_release(int s, struct partition *p, int c, int task_id, 
    int op_id, struct elem *e)
{
  // find out lock on the owners list
  struct lock_entry *l;
  LIST_FOREACH(l, &e->owners, next) {
    if (l->s == c && l->task_id == task_id && l->op_id == op_id)
      break;
  }

  if (l) {
    // free lock
    dprint("srv(%d): cl %d key %" PRIu64 " rc %" PRIu64 
        " being removed from owners\n", s, c, e->key, e->ref_count);

    LIST_REMOVE(l, next);

    mp_release_value_(p, e);
  } else {
    // it is possible that lock is on waiters list. Imagine a scenario
    // where we send requests in bulk to 2 servers, one waits and other
    // aborts. In this case, we will get release message for a request
    // that is currently waiting
    LIST_FOREACH(l, &e->waiters, next) {
      if (l->s == c && l->task_id == task_id && l->op_id == op_id)
        break;
    }

    // can't be on neither owners nor waiters!
    assert(l);
    LIST_REMOVE(l, next);
  }

  plmalloc_free(p, l, sizeof(struct lock_entry));

  // if there are no more owners, then refcount should be 1
  if (LIST_EMPTY(&e->owners) && e->ref_count != 1) {
    printf("found key %"PRIu64" with ref count %d\n", e->key, e->ref_count);
    fflush(stdout);
    assert(0);
  }

  /* If lock_free is set, that means the new lock mode is decided by 
   * the head of waiter list. If lock_free is not set, we still have
   * some readers. So only pending readers can be allowed. Keep 
   * popping items from wait list as long as we have readers.
   */
  l = LIST_FIRST(&e->waiters);
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
    LIST_REMOVE(l, next);
    LIST_INSERT_HEAD(&e->owners, l, next);

    // mark as ready and send message to server
    l->ready = 1;
    mp_send_reply(s, l->s, l->task_id, l->op_id, e);

    dprint("srv(%d): release lock request for key %"PRIu64" marking %d as ready\n", 
        s, e->key, l->s);

    // go to next element
    l = LIST_FIRST(&e->waiters);
  }
}
#endif

void no_wait_release(struct partition *p, struct elem *e)
{
  mp_release_value_(p, e);
}

int no_wait_check_acquire(struct elem *e, char optype)
{
  int r;

  if (optype == OPTYPE_LOOKUP) {
    if (!is_value_ready(e)) {
      r = LOCK_ABORT;
    } else {
      r = LOCK_SUCCESS;
    }
  } else {
    assert(optype == OPTYPE_UPDATE || optype == OPTYPE_INSERT);
    if (!is_value_ready(e) || e->ref_count > 1) {
      r = LOCK_ABORT;
    } else {
      r = LOCK_SUCCESS;
    }
  }

  return r;
}

int no_wait_acquire(struct elem *e, char optype)
{
  int r = no_wait_check_acquire(e, optype);

  if (r == LOCK_ABORT)
    return r;

  if (optype == OPTYPE_LOOKUP) {
    e->ref_count++;
  } else {
    assert(optype == OPTYPE_UPDATE || optype == OPTYPE_INSERT);
    e->ref_count = DATA_READY_MASK | 2;
  }

  return LOCK_SUCCESS;
}

