#include "headers.h"

#if TICKET_LOCK

void tlock_init(tlock_t *lock)
{
  lock->ticket = lock->users = 0;
}

void tlock_acquire(tlock_t *t)
{
  unsigned short me = __sync_fetch_and_add(&t->users, 1);
  
  while (t->ticket != me) _mm_pause();
}

void tlock_release(tlock_t *t)
{
  __sync_fetch_and_add(&t->ticket, 1);
}

#endif
