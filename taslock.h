#ifndef __TLOCK_H__
#define __TLOCK_H__

void taslock_init(taslock_t *lock);
void taslock_acquire(taslock_t *lock);
void taslock_release(taslock_t *lock);

#endif
