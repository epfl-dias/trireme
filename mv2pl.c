#include "headers.h"
#include "smphashtable.h"
#include "plmalloc.h"

#if ENABLE_MV2PL

int set_read_lock(volatile int64_t *val, char use_phys_synch)
{
    int64_t old, new;
    old = *val;

    while (1) {
        if (old == -1)
            return LOCK_ABORT;

        if (use_phys_synch) {
            new = __sync_val_compare_and_swap(val, old, old + 1);
        } else {
            new = old;
            *val = old + 1;
        }

        if (new == old)
            break;

        assert(use_phys_synch);
        old = new;
    }

    assert(old == new && *val != -1);

    return LOCK_SUCCESS;
}

void clear_lock(char optype, struct elem *e, char is_certified,
        char use_phys_synch)
{
    switch(optype) {
        case OPTYPE_LOOKUP:
            assert(e->rd_counter > 0);

            int64_t old_val;
            if (use_phys_synch) {
                old_val = __sync_fetch_and_sub(&e->rd_counter, 1);
            } else {
                old_val = e->rd_counter;
                e->rd_counter--;
            }

            assert(old_val >= 0);
            break;

        case OPTYPE_UPDATE:
            if (is_certified) {
                assert (e->rd_counter == -1);
                e->rd_counter = 0;
            }

            assert(e->is_write_locked);
            e->is_write_locked = 0;

            break;
    }
}

int certify_write(struct elem *e, char use_phys_synch)
{
    int64_t val = e->rd_counter;
    if (val > 0)
        return LOCK_ABORT;

    assert(e->is_write_locked);

    if (use_phys_synch) {
        return __sync_bool_compare_and_swap(&e->rd_counter, val, -1) ?
            LOCK_SUCCESS : LOCK_ABORT;
    }

    return LOCK_SUCCESS;
}

struct elem *mv2pl_acquire(struct partition *p, struct elem *e, char optype)
{
    struct elem *target = NULL;
    int s = p - &hash_table->partitions[0];
    int g_tid = p->current_task->g_tid;

    dprint("srv(%d): %s lock request for key %"PRIu64"\n", s,
            optype == OPTYPE_LOOKUP ? "lookup":"update", e->key);

    if (optype == OPTYPE_LOOKUP) {
        if (set_read_lock(&e->rd_counter, USE_PHYS_SYNCH) == LOCK_SUCCESS)
            target = e;
    } else {
        assert(optype == OPTYPE_UPDATE);

        if (__sync_bool_compare_and_swap(&e->is_write_locked, 0, 1)) {
            target = e;
        }
    }

    return target;
}

void mv2pl_abort(struct task *ctask, struct hash_table *hash_table, int s)
{
    struct txn_ctx *ctx = &ctask->txn_ctx;
    struct partition *p = &hash_table->partitions[s];
    int nops = ctx->nops;

    for (int i = 0; i < nops; i++) {
        struct op_ctx *octx = &ctx->op_ctx[i];

        clear_lock(octx->optype, octx->e, /* is_certified = 0 */ 0,
                USE_PHYS_SYNCH);
    }
}

int mv2pl_validate(struct task *ctask, struct hash_table *hash_table, int s)
{
    /* latch, reset ref count to free the logical lock, unlatch */
    struct txn_ctx *ctx = &ctask->txn_ctx;
    struct partition *p = &hash_table->partitions[s];
    int nops = ctx->nops;
    int r = LOCK_SUCCESS;
    int last_certified_wt = -1;

    /* go through operations validating reads and acquiring certify locks */
    for (int i = 0; i < nops; i++) {
        struct op_ctx *octx = &ctx->op_ctx[i];
        if (octx->optype == OPTYPE_UPDATE) {
            if ((r = certify_write(ctx->op_ctx[i].e, USE_PHYS_SYNCH)) !=
                    LOCK_SUCCESS)
                break;

            last_certified_wt = i;
        }
    }

    for (int i = 0; i < nops; i++) {
        struct op_ctx *octx = &ctx->op_ctx[i];

        /* if we have all certify locks, update data */
        if (r == LOCK_SUCCESS && octx->optype == OPTYPE_UPDATE)
            memcpy(octx->e->value, octx->data_copy->value, octx->e->size);

        /* release lock */
        clear_lock(octx->optype, octx->e, i <= last_certified_wt,
                USE_PHYS_SYNCH);
    }

    if (r == LOCK_SUCCESS) {
        dprint("srv(%d):mv2pl validation success\n", s);
        p->nvalidate_success++;
        return TXN_COMMIT;
    }

    dprint("srv(%d):mv2pl validation failed\n", s);
    p->nvalidate_failure++;
    return TXN_ABORT;
}

int del_mv2pl_acquire(struct elem *e, char optype)
{
    int r;

    switch (optype) {
        case OPTYPE_LOOKUP:
            r = set_read_lock(&e->rd_counter, !USE_PHYS_SYNCH);
            break;

        case OPTYPE_UPDATE:
            if (e->is_write_locked) {
                r = LOCK_ABORT;
            } else {
                e->is_write_locked = 1;
                r = LOCK_SUCCESS;
            }
            break;

        case OPTYPE_CERTIFY:
            assert(e->is_write_locked);

            r = certify_write(e, !USE_PHYS_SYNCH);
            break;

        default:
            assert(0);
    };

    return r;
}

int del_mv2pl_release(struct partition *p, struct elem *e, char optype)
{
    /* XXX: With delegation, we have to extend the release message to encode
     * the type of release (rd or wt). For now, we will assume that its either
     * all reads or all writes. Need to change this later.
     */
    int r;

    switch(optype) {
        case OPTYPE_LOOKUP:
            clear_lock(OPTYPE_LOOKUP, e, 0, !USE_PHYS_SYNCH);
            break;

        case OPTYPE_UPDATE:
            clear_lock(OPTYPE_UPDATE, e, 0, !USE_PHYS_SYNCH);
            break;

        case OPTYPE_CERTIFY:
            clear_lock(OPTYPE_UPDATE, e, 1, !USE_PHYS_SYNCH);
            break;
    };

    return r;
}

int del_mv2pl_check_acquire(struct elem *e, char optype)
{
    int r = LOCK_SUCCESS;

    switch(optype) {
        case OPTYPE_LOOKUP:
            if (e->rd_counter == -1)
                r = LOCK_ABORT;
            break;

        case OPTYPE_UPDATE:
            if (e->is_write_locked)
                r = LOCK_ABORT;
            break;

        case OPTYPE_CERTIFY:
            assert(e->is_write_locked);
            if (e->rd_counter > 1)
                r = LOCK_ABORT;
            break;
    }

    return r;
}

int del_mv2pl_validate(struct task *ctask, struct hash_table *hash_table, int s)
{
    /* latch, reset ref count to free the logical lock, unlatch */
    struct txn_ctx *ctx = &ctask->txn_ctx;
    struct partition *p = &hash_table->partitions[s];
    int nops = ctx->nops;
    int r = LOCK_SUCCESS;
    int last_certified_wt = -1;

    /* go through operations validating reads and acquiring certify locks */
    for (int i = 0; i < nops; i++) {
        struct op_ctx *octx = &ctx->op_ctx[i];
        if (octx->optype == OPTYPE_UPDATE) {
            if (octx->target == s) {
                // local requests. directly try to certify the write
                if ((r = certify_write(ctx->op_ctx[i].e, USE_PHYS_SYNCH)) !=
                        LOCK_SUCCESS)
                    break;
            } else {
                // send a message asking to ceritfy
                // XXX: Here, we send one at a time, we can batch it all and
                // send it together in one shot which will optimize things.
                struct hash_op op, *pop[1];
                pop[0] = &op;
                op.optype = OPTYPE_CERTIFY;
                op.size = 0;
                op.key = octx->e->key;

                void *pres[1];
                pres[0] = NULL;
                smp_hash_doall(ctask, hash_table, s, octx->target, 1, pop, pres, 0);

                if (!pres[0]) {
                    // certify operation failed
                    r = LOCK_ABORT;
                    break;
                } else {
                    // certify succeeded
                    assert(pres[0] == octx->e);
                }
            }

            last_certified_wt = i;
        }
    }

    for (int i = 0; i < nops; i++) {
        struct op_ctx *octx = &ctx->op_ctx[i];

        /* if we have all certify locks, update data */
        if (r == LOCK_SUCCESS && octx->optype == OPTYPE_UPDATE)
            memcpy(octx->e->value, octx->data_copy->value, octx->e->size);

        /* release lock */
        if (octx->target == s) {
            // local lock clear
            clear_lock(octx->optype, octx->e, i <= last_certified_wt,
                    USE_PHYS_SYNCH);
        } else {
            // remote lock clear
            if (octx->optype == OPTYPE_LOOKUP) {
                mp_mark_ready(hash_table, s, octx->target, ctask->tid, 0, octx->e,
                        OPTYPE_LOOKUP);
            } else {
                // request a cert release only if we are sure we got a cert lock
                mp_mark_ready(hash_table, s, octx->target, ctask->tid, 0, octx->e,
                        i <= last_certified_wt ? OPTYPE_CERTIFY : OPTYPE_UPDATE);
            }
        }
    }

    if (r == LOCK_SUCCESS) {
        dprint("srv(%d):mv2pl validation success\n", s);
        p->nvalidate_success++;
        return TXN_COMMIT;
    }

    dprint("srv(%d):mv2pl validation failed\n", s);
    p->nvalidate_failure++;
    return TXN_ABORT;

}

#endif //ENABLE_MV2PL
