#include "headers.h"

#if TAS_LOCK

#define TASLOCK_BUSY 1
#define TASLOCK_FREE 0

void taslock_init(taslock_t *lock)
{
  *lock = TASLOCK_FREE;
}

void taslock_acquire(taslock_t *t)
{
  
  while (1) {
    if (!xchg_32(t, TASLOCK_BUSY))
      return;

    while (*t)
      _mm_pause();
  }
}

void taslock_release(taslock_t *t)
{
  COMPILER_BARRIER();
  *t = TASLOCK_FREE;
}

#endif
