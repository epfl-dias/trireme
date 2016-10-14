#ifndef __RW_LOCK_H__
#define __RW_LOCK_H__

void rwlock_init(rwlock_t *lock);
int rwlock_wrtrylock(rwlock_t *l);
int rwlock_rdtrylock(rwlock_t *l);
void rwlock_rdlock(rwlock_t *l);
void rwlock_wrunlock(rwlock_t *l);
void rwlock_rdunlock(rwlock_t *l);

#endif
