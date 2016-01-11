#ifndef __TLOCK_H__
#define __TLOCK_H__

void tlock_init(tlock_t *lock);
void tlock_acquire(tlock_t *lock);
void tlock_release(tlock_t *lock);

#endif
