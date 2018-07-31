#include "headers.h"

#if ANDERSON_LOCK
void alock_init(alock_t *al, int nthread)
{
  assert(al);
  assert(nthread <= MAX_SERVERS);
  al->has_lock[0].x = 1;
  al->nthread = nthread;
  al->next_slot = 0;
}

void alock_acquire(alock_t *lock, int *extra)
{
printf("in alock acquire procedure\n");
  int me = __sync_fetch_and_add(&lock->next_slot, 1);
  if(me > 0 && (me % lock->nthread) == 0)
    __sync_fetch_and_add(&lock->next_slot, -lock->nthread);
  me = me % lock->nthread;
  while(lock->has_lock[me].x == 0) {
    _mm_pause();
  }
  lock->has_lock[me].x = 0;
  *extra = me;
}

void alock_release(alock_t *lock, int *extra)
{
  printf("in alock release procedure\n");
  int me = *extra;
  lock->has_lock[(me + 1) % lock->nthread].x = 1;
}
#endif
