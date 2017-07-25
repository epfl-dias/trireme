#include "headers.h"
#include "smphashtable.h"
#include "plmalloc.h"

#if ENABLE_SVDREADLOCK_CC

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

        if (e->owners[index] == owner && deps[me]) {
            /* deadlock detected. break out */
            cycle_found = 1;
        }

    } else {

        /* if owner is not blocked, there cannot be a deadlock. 
         * so simply spin waiting for owner to be cleared if owner is not
         * blocked
         */
        while (powner->waiting_for == -1 && e->owners[index] == owner)
            _mm_pause();
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
            int64_t owner = __sync_val_compare_and_swap(&e->owners[s], -1, s);
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

        /* spin on each lock until we get it or we detect a deadlock */
        for (int i = 0; i < NCORES; i++) {
            do {
                int64_t owner = __sync_val_compare_and_swap(&e->owners[i], -1, s);
                if (owner == -1) {
                    nlocks++;
                    break;
                }

                assert(owner < g_nservers);

                struct partition *powner = &hash_table->partitions[owner];

                /* if the owner is blocked, we're blocked on owner */
                p->waiting_for = owner;

                if (cycle_check(e, i, s, owner)) {
                    cycle_found = 1;
                    break;
                }

            } while(1);

            if (cycle_found)
                break;
        }

        /* if we got all locks, then we're good. Otherwise, release whatever
         * we got and fail
         */
        if (nlocks == NCORES) {
            target = e;
        } else {

            for (int i = nlocks - 1; i >= 0; i--) {
                e->owners[i] = -1;
            }
        }
    }

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
            assert(octx->e->owners[s] == s);
            octx->e->owners[s] = -1;
            break;

        case OPTYPE_UPDATE:
            for (int j = NCORES - 1; j >= 0; j--) {
                assert(octx->e->owners[j] == s);
                octx->e->owners[j] = -1;
            }
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

    /* go through operations validating reads and acquiring certify locks */
    for (int i = 0; i < nops; i++) {
        struct op_ctx *octx = &ctx->op_ctx[i];

        /* if we have all certify locks, update data */
        switch(octx->optype) {
        case OPTYPE_LOOKUP:
            assert(octx->e->owners[s] == s);
            octx->e->owners[s] = -1;
            break;

        case OPTYPE_UPDATE:
            if (r == LOCK_SUCCESS && octx->optype == OPTYPE_UPDATE)
                memcpy(octx->e->value, octx->data_copy->value, octx->e->size);

            for (int j = NCORES - 1; j >= 0; j--) {
                assert(octx->e->owners[j] == s);
                octx->e->owners[j] = -1;
            }
        }
    }

    return TXN_COMMIT;
}

#endif //ENABLE_SVDREADLOCK
