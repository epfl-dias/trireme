#include "headers.h"
#include "smphashtable.h"
#include "benchmark.h"

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

extern struct benchmark *g_benchmark;
extern int write_threshold;

int selock_nowait_acquire(struct elem *e, char optype)
{
  /* latch the record. check to see if it is a conflicting lock mode
   * if so, bad luck. we just fail
   */
  int r = 0;
  int alock_state;

#if SE_LATCH
  LATCH_ACQUIRE(&e->latch, &alock_state);

  /* if there are no conflicting locks, we set ref count to indicate 
   * lock type
   */
  if (is_value_ready(e)) {
    if (optype == OPTYPE_LOOKUP) {
   	  e->ref_count++;
      r = 1;
    } else {
      assert(optype == OPTYPE_UPDATE);
      
      if (e->ref_count == 1) {
        e->ref_count = DATA_READY_MASK | 2;
        r = 1;
      }
    }

  }

  LATCH_RELEASE(&e->latch, &alock_state);
#else
  assert(g_benchmark == &micro_bench && write_threshold == 100);
  r = 1;
#endif

  return r;
}

void selock_nowait_release(struct elem *e)
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

int selock_acquire(struct elem *e, char optype)
{
  return selock_nowait_acquire(e, optype);
}

void selock_release(struct elem *e)
{
  return selock_nowait_release(e);
}

#endif
