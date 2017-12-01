#include "headers.h"
#include "smphashtable.h"
#include "plmalloc.h"

#if ENABLE_MV2PL_DRWLOCK

void clear_lock(int s, char optype, struct elem *e, char is_certified)
{
    switch(optype) {
        case OPTYPE_LOOKUP:
            assert(e->rd_counter[s].value);
            e->rd_counter[s].value = 0;
            break;

        case OPTYPE_UPDATE:
            if (is_certified) {
                for (int i = g_nservers - 1; i >= 0; i--) {
                    assert (e->rd_counter[i].value);
                    e->rd_counter[i].value = 0;
                }
            }

            assert(e->is_write_locked);
            e->is_write_locked = 0;

            break;
    }
}

int certify_write(struct elem *e)
{
    assert(e->is_write_locked);

    int ncert = 0;
    for (; ncert < g_nservers; ncert++) {
        if (!__sync_bool_compare_and_swap(&e->rd_counter[ncert].value, 0, 1))
            break;
    }

    if (ncert != g_nservers) {
        while (--ncert >= 0)
            e->rd_counter[ncert].value = 0;

        return LOCK_ABORT;
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
        if (__sync_bool_compare_and_swap(&e->rd_counter[s].value, 0, 1))
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

        clear_lock(s, octx->optype, octx->e, /* is_certified = 0 */ 0);
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
            if ((r = certify_write(ctx->op_ctx[i].e)) != LOCK_SUCCESS)
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
        clear_lock(s, octx->optype, octx->e, i <= last_certified_wt);
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

#endif //ENABLE_MV2PL_DRWLOCK
