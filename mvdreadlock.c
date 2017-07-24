#include "headers.h"
#include "smphashtable.h"
#include "plmalloc.h"

#if ENABLE_MVDREADLOCK_CC

int cycle_check(struct elem *e, int me, int owner)
{
    char cycle_found = 0;
    char deps[MAX_SERVERS];

    memset(deps, 0, MAX_SERVERS);
    
    int srv = owner;

    do {
        if (srv == -1)
            break;

        deps[srv] = 1;

        if (srv == me)
            break;

        struct partition *powner = &hash_table->partitions[srv];
        if (powner->waiting_for == -1)
            break;

        srv = powner->waiting_for;
    } while(1);

    /* if current owner still the same and if we detected a cycle,
     * then raise alarm
     */
    if (e->owner == owner && deps[me])
        cycle_found = 1;

    return cycle_found;
}

struct elem *mvdreadlock_acquire(struct partition *p, struct elem *e,
        char optype)
{
    struct elem *target = NULL;
    int s = p - &hash_table->partitions[0];
    int g_tid = p->current_task->g_tid;

    dprint("srv(%d): %s lock request for key %"PRIu64"\n", s,
            optype == OPTYPE_LOOKUP ? "lookup":"update", e->key);

    if (optype == OPTYPE_LOOKUP) {
        assert(0);
    } else {
        assert(optype == OPTYPE_UPDATE);

        do {
            int64_t owner = __sync_val_compare_and_swap(&e->owner, -1, s);
            if (owner == -1) {
                target = e;
                break;
            }

            assert(owner < g_nservers);

            struct partition *powner = &hash_table->partitions[owner];
            
            /* if the owner is blocked, we're blocked on owner */
            p->waiting_for = owner;

            /* if owner is blocked, do cycle check. */
            if (powner->waiting_for != -1) {

                if (cycle_check(e, s, owner)) {
                    /* deadlock detected. break out */
                    break;
                }

             } else {

                /* if owner is not blocked, there cannot be a deadlock. 
                 * so simply spin waiting for owner to be cleared if owner is not
                 * blocked
                 */
                while (powner->waiting_for == -1 && e->owner == owner)
                    _mm_pause();
            }

        } while (1);
    }

    /* we are no longer blocked either way */
    p->waiting_for = -1;

    return target;
}

void mvdreadlock_abort(struct task *ctask, struct hash_table *hash_table, int s)
{
    struct txn_ctx *ctx = &ctask->txn_ctx;
    struct partition *p = &hash_table->partitions[s];
    int nops = ctx->nops;

    for (int i = 0; i < nops; i++) {
        struct op_ctx *octx = &ctx->op_ctx[i];
        assert(octx->e->owner == s);
        octx->e->owner = -1;
    }

    return;
}

int mvdreadlock_validate(struct task *ctask, struct hash_table *hash_table, int s)
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
        if (r == LOCK_SUCCESS && octx->optype == OPTYPE_UPDATE)
            memcpy(octx->e->value, octx->data_copy->value, octx->e->size);

        /* release lock */
        assert(octx->e->owner == s);

        octx->e->owner = -1;
    }

    return TXN_COMMIT;
}

#endif //ENABLE_MV2PL
