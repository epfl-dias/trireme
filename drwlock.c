#include "headers.h"

#if DRW_LOCK

#define SIMPLE_SPINLOCK 1

typedef unsigned int sspinlock_t;

#include "sspinlock.c"

void drwlock_init(drwlock_t *l)
{
    for (int i = 0; i < NCORES; i++)
        sspinlock_init(&(l->latch[i].spinlock));
}

int drwlock_rdtrylock(int s, drwlock_t *l)
{
    /* Try to get write lock */
    if (sspinlock_trylock(&(l->latch[s].spinlock)))
        return 0;

    /* Success! */
    return 1;
}

void drwlock_rdunlock(int s, drwlock_t *l)
{
    sspinlock_release(&(l->latch[s].spinlock));
}

int drwlock_wrtrylock(drwlock_t *l)
{

    if (sspinlock_trylock(&(l->latch[0].spinlock)))
        return 0;

    for (int i = 1; i < NCORES; i++)
        sspinlock_acquire(&(l->latch[i].spinlock));

    return 1;
}

void drwlock_wrunlock(drwlock_t *l)
{
    for (int i = NCORES - 1; i >= 0; i--)
        sspinlock_release(&(l->latch[i].spinlock));
}

#undef SIMPLE_SPINLOCK

#endif //DRW_LOCK

