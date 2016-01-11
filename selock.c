#include "headers.h"
#include "smphashtable.h"

#if 0
enum lock_t {LOCK_EXCL, LOCK_SHRD, LOCK_NONE};

struct lock_item {
  lock_t type;

  TAILQ_ENTRY(lock_item) next_owner;
  TAILQ_ENTRY(lock_item) next_waiter;
};
#endif

#if SHARED_EVERYTHING

int selock_nowait_acquire(struct elem *e, char optype)
{
  /* latch the record. check to see if it is a conflicting lock mode
   * if so, bad luck. we just fail
   */
  int r = 0;

  LATCH_ACQUIRE(&e->latch);

  /* if there are no conflicting locks, we set ref count to indicate 
   * lock type
   */
  if (is_value_ready(e)) {
    if (optype == OPTYPE_LOOKUP) {
   	  e->ref_count++;
    } else {
      assert(optype == OPTYPE_UPDATE);
      e->ref_count = DATA_READY_MASK | 2;
    }

    r = 1;
  }

  LATCH_RELEASE(&e->latch);

  return r;
}

void selock_nowait_release(struct elem *e)
{
  /* latch, reset ref count to free the logical lock, unlatch */

  LATCH_ACQUIRE(&e->latch);

  e->ref_count = (e->ref_count & (~DATA_READY_MASK)) - 1;

  LATCH_RELEASE(&e->latch);
}

int selock_acquire(struct elem *e, char optype)
{
  return selock_nowait_acquire(e, optype);
}

void selock_release(struct elem *e)
{
  return selock_nowait_release(e);
}

#endif
