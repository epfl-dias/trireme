#ifndef __ALOCK_H__
#define __ALOCK_H__

void alock_init(alock_t *alock, int nthread);
void alock_acquire(alock_t *lock, int *extra);
void alock_release(alock_t *lock, int *extra);

#endif
