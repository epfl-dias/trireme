#include "headers.h"

#if CUSTOM_RW_LOCK

#define SIMPLE_SPINLOCK 1

typedef unsigned int sspinlock_t;

#include "sspinlock.c"

void rwlock_init(rwlock_t *l)
{
    sspinlock_init(&l->spinlock);
    l->readers = 0;
}

void rwlock_wrunlock(rwlock_t *l)
{
    sspinlock_release(&l->spinlock);
}

int rwlock_wrtrylock(rwlock_t *l)
{
    /* Want no readers */
    if (l->readers) return 0;
    
    /* Try to get write lock */
    if (sspinlock_trylock(&l->spinlock)) return 0;
    
    if (l->readers)
    {
        /* Oops, a reader started */
        sspinlock_release(&l->spinlock);
        return 0;
    }
    
    /* Success! */
    return 1;
}

void rwlock_rdunlock(rwlock_t *l)
{
    __sync_add_and_fetch(&l->readers, -1);
}

int rwlock_rdtrylock(rwlock_t *l)
{
    /* Speculatively take read lock */
    __sync_add_and_fetch(&l->readers, 1);
        
    /* Success? */
    if (!l->spinlock) return 1;
    
    /* Failure - undo */
    __sync_add_and_fetch(&l->readers, -1);
    
    return 0;
}

#undef SIMPLE_SPINLOCK

#endif //CUSTOM_RW_LOCK

