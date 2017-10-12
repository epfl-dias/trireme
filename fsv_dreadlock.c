#include "headers.h"
#include "smphashtable.h"
#include "plmalloc.h"

#if ENABLE_FSVDREADLOCK_CC

void gather_deps(char *deps, int me, int owner)
{
    int srv = owner;

    do {
        if (srv == -1)
            break;

        deps[srv] = 1;

        if (srv == me)
            return;

        struct partition *powner = &hash_table->partitions[srv];
        if (powner->waiting_for == -1)
            return;

        srv = powner->waiting_for;
    } while(1);
}

int cycle_check(struct elem *e, int index, int me, int owner)
{
    char cycle_found = 0;
    char deps[MAX_SERVERS];
    memset(deps, 0, MAX_SERVERS);
 
    assert(owner < g_nservers);

    struct partition *powner = &hash_table->partitions[owner];

    /* if owner is blocked, do cycle check. */
    if (powner->waiting_for != -1) {

        gather_deps(deps, me, owner);

        if (e->owners[index].id == owner && deps[me]) {
            /* deadlock detected. break out */
            cycle_found = 1;
        }

    } 

    return cycle_found;

}

struct elem *svdreadlock_acquire(struct partition *p, struct elem *e,
        char optype)
{
    struct elem *target = NULL;
    int s = p - &hash_table->partitions[0];
    int g_tid = p->current_task->g_tid;
    int nlocks = 0;
    char cycle_found = 0;

    dprint("srv(%d): %s lock request for key %"PRIu64"\n", s,
            optype == OPTYPE_LOOKUP ? "lookup":"update", e->key);

    if (optype == OPTYPE_LOOKUP) {
        /* try to get a read lock. If we can get it, good. If not, we just
         * spin until we detect a cycle or we get the read lock
         */
        do {
            int64_t owner = 
                __sync_val_compare_and_swap(&e->owners[s].id, -1, s);

            if (owner == -1) {
                nlocks++;
                break;
            }

            /* if the owner is blocked, we're blocked on owner */
            p->waiting_for = owner;

            if (cycle_check(e, s, s, owner)) {
                cycle_found = 1;
                break;
            }

        } while (1);
        
        if (nlocks) {
            assert(!cycle_found);
            target = e;
        }
    } else {
        assert(optype == OPTYPE_UPDATE);

        /* first get the ticket */
        uint32_t me = __sync_fetch_and_add(&e->tlock.users, 1);

        dprint("srv(%d): %s lock request for key %"PRIu64
                " me %u, ticket %u\n", s,
                optype == OPTYPE_LOOKUP ? "lookup":"update", e->key, me,
                e->tlock.ticket);

        while (1) {
            volatile uint32_t cur = e->tlock.ticket;

            /* if we are the lotter winner, break out and start trying
             * atomics
             */
            if (cur == me) {
                /* no longer waiting. so reset waiter */
                e->waiters[me % NCORES] = -1;
                break;
            }

            /* not lotter winner. add ourself to the waiters array */
            if (e->waiters[me % NCORES] == -1) {
                e->waiters[me % NCORES] = s;
            } else {
                assert(e->waiters[me % NCORES] == s);
            }

            /* There's a writer before us. we need to wait until we are
             * signalled to go forward.
             */
            int owner = e->owners[0].id;
            p->waiting_for = owner;
            if (owner != -1) {
                assert(owner < g_nservers);

                dprint("srv(%d): update lock request for key %"PRIu64
                        " checking for cycles in ticket wait\n", s, e->key);

                if (cycle_check(e, 0, s, owner)) {
                    dprint("srv(%d): update lock request for key %"PRIu64
                            " me %u, ticket %u, cycle found in ticket wait\n", s,
                            e->key, me, e->tlock.ticket);

                    cycle_found = 1;
                    e->waiters[me % NCORES] = -1;
                    goto done;
                }
            }
        }

        /* spin on each lock until we get it or we detect a deadlock */
        for (int i = 0; i < g_nservers; i++) {
            if (cycle_found)
                break;

            do {
                int cur_owner = -1;
                if (e->owners[i].id == s) {
                    /* lock is being passed down.  */
                } else {
                    /* this is the first write. so try the atomic */
                    cur_owner = __sync_val_compare_and_swap(&e->owners[i].id, -1, s);
                }

                if (cur_owner == -1) {
                    /* we have this lock. move ahead */
                    nlocks++;
                    break;
                }

                /* if the owner is blocked, we're blocked on owner */
                assert(cur_owner < g_nservers);

                p->waiting_for = cur_owner;

                dprint("srv(%d): update lock request for key %"PRIu64
                        " checking for cycles in counter wait\n", s, e->key);

                if (cycle_check(e, i, s, cur_owner)) {
                    dprint("srv(%d): update lock request for key %"PRIu64
                            " cycle found in owner counter wait\n", s, e->key);

                    cycle_found = 1;
                    break;
                }

            } while(1);
        }

        /* if we got all locks, then we're good. Otherwise, release whatever
         * we got and fail. In any case, we did get the ticket and we are in the
         * front of the writer list. so we should pass on the ticket to next
         * writer.
         */
        if (nlocks == g_nservers) {
            target = e;
        } else {
            COMPILER_BARRIER();

            /* it is possible for a thread to get a ticket, never win the
             * lottery, end up detecting a cycle and aborting. In such a case,
             * those ticket entries would not have any thread waiting.
             * So we skip those entries
             */
            do {
                e->tlock.ticket++;

                if (e->waiters[e->tlock.ticket % NCORES] != -1)
                    break;
            } while (1);

            COMPILER_BARRIER();

            for (int i = nlocks - 1; i >= 0; i--) {
                e->owners[i].id = -1;
            }
        }
    }

done:
    /* we are no longer blocked either way */
    p->waiting_for = -1;

    return target;
}

void svdreadlock_abort(struct task *ctask, struct hash_table *hash_table, int s)
{
    struct txn_ctx *ctx = &ctask->txn_ctx;
    struct partition *p = &hash_table->partitions[s];
    int nops = ctx->nops;

    for (int i = 0; i < nops; i++) {
        struct op_ctx *octx = &ctx->op_ctx[i];
        switch(octx->optype) {
        case OPTYPE_LOOKUP:
            assert(octx->e->owners[s].id == s);
            assert(0);
            octx->e->owners[s].id = -1;
            break;

        case OPTYPE_UPDATE:
            dprint("srv(%d): update releasing lock for key %"PRIu64
                    " ticket %u\n", s, octx->e->key, octx->e->tlock.ticket);

            int new_owner = -1;
            uint32_t ticket = octx->e->tlock.ticket;
            if ((ticket + 1) == octx->e->tlock.users) {
                /* no more writer. so release lock */
            } else {
                /* some one is waiting. so set counters to that person's id */
                new_owner = octx->e->waiters[(ticket + 1) % NCORES];
            }

            /* update ownership */
            for (int j = g_nservers - 1; j >= 0; j--) {
                assert(octx->e->owners[j].id == s);
                octx->e->owners[j].id = new_owner;
            }

            /* pass ticket to the new owner */
            COMPILER_BARRIER();
            octx->e->tlock.ticket++;

        }        
    }

    return;
}

int svdreadlock_validate(struct task *ctask, struct hash_table *hash_table, int s)
{
    /* latch, reset ref count to free the logical lock, unlatch */
    struct txn_ctx *ctx = &ctask->txn_ctx;
    struct partition *p = &hash_table->partitions[s];
    int nops = ctx->nops;
    int r = LOCK_SUCCESS;

    for (int i = 0; i < nops; i++) {
        struct op_ctx *octx = &ctx->op_ctx[i];

        switch(octx->optype) {
        case OPTYPE_LOOKUP:
            assert(0);
            assert(octx->e->owners[s].id == s);
            octx->e->owners[s].id = -1;

            break;

        case OPTYPE_UPDATE:
            if (r == LOCK_SUCCESS && octx->optype == OPTYPE_UPDATE)
                memcpy(octx->e->value, octx->data_copy->value, octx->e->size);

            dprint("srv(%d): update releasing lock for key %"PRIu64
                    " ticket %u\n", s, octx->e->key, octx->e->tlock.ticket);
        
            int new_owner = -1;
            uint32_t ticket = octx->e->tlock.ticket;
            if ((ticket + 1) == octx->e->tlock.users) {
                /* no more writer. so release lock */
            } else {
                /* some one is waiting. so set counters to that person's id */
                do {
                    new_owner = octx->e->waiters[(ticket + 1) % NCORES];
                } while (new_owner == -1);
            }

            /* update ownership */
            for (int j = g_nservers - 1; j >= 0; j--) {
                assert(octx->e->owners[j].id == s);
                octx->e->owners[j].id = new_owner;
            }

            /* pass ticket to the new owner */
            COMPILER_BARRIER();
            octx->e->tlock.ticket++;
        }
    }

    return TXN_COMMIT;
}

#endif
