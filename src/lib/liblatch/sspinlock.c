#include "headers.h"

#if SIMPLE_SPINLOCK

#define EBUSY 1

void sspinlock_init(sspinlock_t *lock)
{
    *lock = 0;
}

void sspinlock_acquire(sspinlock_t *lock)
{
    while (1) {
        if (!xchg_32(lock, EBUSY))
            return;

        while (*lock) _mm_pause();
    }
}

void sspinlock_release(sspinlock_t *lock)
{
    COMPILER_BARRIER();
    *lock = 0;
}

int sspinlock_trylock(sspinlock_t *lock)
{
    return xchg_32(lock, EBUSY);
}

#endif
