#include <sys/queue.h>
#include <assert.h>

#include "hashprotocol.h"
#include "partition.h"
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

int selock_nowait_acquire(struct partition *p, char optype, 
    struct elem *e)
{
  /* latch the record. check to see if it is a conflicting lock mode
   * if so, bad luck. we just fail
   */
  int r = 0;

  pthread_mutex_lock(&e->latch);

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

  pthread_mutex_unlock(&e->latch);

  return r;
}

void selock_nowait_release(struct partition *p, struct elem *e)
{
  /* latch, reset ref count to free the logical lock, unlatch */

  pthread_mutex_lock(&e->latch);

  e->ref_count = (e->ref_count & (~DATA_READY_MASK)) - 1;

  assert(e->ref_count > 0);

  pthread_mutex_unlock(&e->latch);
}

int selock_acquire(struct partition *p, char optype, struct elem *e)
{
  return selock_nowait_acquire(p, optype, e);
}

void selock_release(struct partition *p, struct elem *e)
{
  return selock_nowait_release(p, e);
}

#endif
