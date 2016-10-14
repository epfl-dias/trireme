#include "headers.h"

#if TICKET_LOCK

#define TICKET_BACKOFF 512

void tlock_init(tlock_t *lock)
{
    lock->ticket = lock->users = 0;
}

void tlock_acquire(tlock_t *t)
{
    uint32_t me = __sync_fetch_and_add(&t->users, 1);
    uint32_t distance_prev = 1;

    while (1) {
        uint32_t cur = t->ticket;

        if (cur == me)
            break;

        uint32_t distance = cur > me ?  (cur - me) : (me - cur);

        if (distance > 1) {
            if (distance != distance_prev) {
                distance_prev = distance;
            }

            nop_rep(distance * TICKET_BACKOFF);
        }
    }
}

void tlock_release(tlock_t *t)
{
    COMPILER_BARRIER();
    t->ticket++;
}

#endif
