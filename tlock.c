#include "headers.h"

#if TICKET_LOCK

void tlock_init(tlock_t *lock)
{
  lock->ticket = lock->users = 0;
}

void ticket_lock(tlock_t *t)
{
  unsigned short me = __sync_fetch_and_add(&t->users, 1);
  
  while (t->ticket != me) _mm_pause();
}

void ticket_unlock(tlock_t *t)
{
  t->ticket++;
}

#endif
