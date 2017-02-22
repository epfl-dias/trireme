#include "headers.h"
#include "smphashtable.h"
#include "benchmark.h"
#include "silo.h"
#include "plmalloc.h"

#if ENABLE_SILO_CC

void silo_latch_acquire(struct elem *e)
{
#if SILO_USE_ATOMICS
    //should not be called
    assert(0);
#else
    LATCH_ACQUIRE(&e->latch, NULL);
#endif
}

void silo_latch_release(struct elem *e)
{
#if SILO_USE_ATOMICS
    assert(e->tid & SILO_LOCK_BIT);
    e->tid = e->tid & (~SILO_LOCK_BIT);
#else
    LATCH_RELEASE(&e->latch, NULL);
#endif
}

static int silo_latch_tryacquire(struct elem *e)
{
#if SILO_USE_ATOMICS
    uint64_t tid = e->tid;
    if (tid & SILO_LOCK_BIT)
        return 0;

    return __sync_bool_compare_and_swap(&e->tid, tid, tid | SILO_LOCK_BIT);
#else
    return !LATCH_TRY_ACQUIRE(&e->latch);
#endif
}

static int validate_row(struct op_ctx *ctx, char is_write)
{
    /* If write, we already have a lock. so all we need to do is simply check
     * if the tid has changed. 
     * If read, we check if someone else has a lock. If so, abort. If not,
     * check the tid
     */
    int r;

#if SILO_USE_ATOMICS
    uint64_t copy_tid = ctx->e_copy->tid, tid = ctx->e->tid;
    if (is_write)
        return ((copy_tid & (~SILO_LOCK_BIT)) == (tid & (~SILO_LOCK_BIT)) ? 
                TXN_COMMIT : TXN_ABORT);

    if (tid & SILO_LOCK_BIT)
        return TXN_ABORT;
    else if (copy_tid != (tid & (~SILO_LOCK_BIT)))
        return TXN_ABORT;
    else
        return TXN_COMMIT;

#else
    if (!is_write) {
        if (!silo_latch_tryacquire(ctx->e))
            return TXN_ABORT;
    }

    if (ctx->e->tid == ctx->e_copy->tid)
        r = TXN_COMMIT;
    else
        r = TXN_ABORT;

    if (!is_write) {
        silo_latch_release(ctx->e);
    }
#endif

    return r;
}

static int preabort_check(int s, struct txn_ctx *ctx, int *write_set,
        int wt_idx, int *read_set, int rd_idx)
{
    for (int i = 0; i < wt_idx; i++) {
        struct op_ctx *octx = &ctx->op_ctx[write_set[i]];
        if (octx->e->tid != octx->e_copy->tid) {
            dprint("srv(%d): preabort check failed on write op %d "
                    "key %"PRId64" tid %d copy tid %d \n", s, write_set[i],
                    octx->e->key, octx->e->tid, octx->e_copy->tid);

            return TXN_ABORT;
        }
    }

    for (int i = 0; i < rd_idx; i++) {
        struct op_ctx *octx = &ctx->op_ctx[read_set[i]];
        if (octx->e->tid != octx->e_copy->tid) {
            dprint("srv(%d): preabort check failed on read op %d "
                    "key %"PRId64" tid %d copy tid %d \n", s, read_set[i],
                    octx->e->key, octx->e->tid, octx->e_copy->tid);

            return TXN_ABORT;
        }
    }

    return TXN_COMMIT;
}

int silo_validate(struct task *ctask, struct hash_table *hash_table, int s)
{
    struct txn_ctx *ctx = &ctask->txn_ctx;
    struct partition *p = &hash_table->partitions[s];
    int nops = ctx->nops;
    int r = TXN_COMMIT;

    int read_set[MAX_OPS_PER_QUERY], write_set[MAX_OPS_PER_QUERY];
    int rd_idx= 0, wt_idx = 0;

    dprint("srv(%d): silo entering validation\n", s);

    for (int i = 0; i < nops; i++) {
        switch (ctx->op_ctx[i].optype) {
            case OPTYPE_LOOKUP:
                read_set[rd_idx++] = i;
                break;

            case OPTYPE_UPDATE:
                write_set[wt_idx++] = i;
                break;

            default:

                // we do not support inserts yet with Silo
                assert(0);
                break;
        }
    }

    // sort write set in key order
    for (int i = 0; i < wt_idx; i++) {
        for (int j = i + 1; j < wt_idx; j++) {
            struct op_ctx *ictx = &ctx->op_ctx[write_set[i]];
            struct op_ctx *jctx = &ctx->op_ctx[write_set[j]];

            if (ictx->e->key > jctx->e->key) {
                int tmp = write_set[i];
                write_set[i] = write_set[j];
                write_set[j] = tmp;
            }
        }
    }


    /* preabort checks */
    if ((r = preabort_check(s, ctx, write_set, wt_idx, read_set, 
                    rd_idx)) == TXN_ABORT) {
        dprint("srv(%d): preabort check failed\n", s);
        goto final;
    }

    /* lock all rows in the write set now */
    char done_locking = 0;
    int nlocks;
    while (!done_locking) {
        nlocks = 0;
        for (int i = 0; i < wt_idx; i++) {
            struct op_ctx *octx = &ctx->op_ctx[write_set[i]];
            if (!silo_latch_tryacquire(octx->e))
                break;

            nlocks++;
            if (octx->e->tid != octx->e_copy->tid) {
                r = TXN_ABORT;
                goto final;
            }
        }

        if (nlocks == wt_idx) {
            done_locking = 1;
        } else {

            // we were not able to get all locks. release, sleep and repeat
            for (int i = 0; i < nlocks; i++) {
                struct op_ctx *octx = &ctx->op_ctx[write_set[i]];
                silo_latch_release(octx->e);
            }

            if ((r = preabort_check(s, ctx, write_set, wt_idx, read_set,
                            rd_idx)) == TXN_ABORT) {
                goto final;
            }
        }
    }

    // at this point we should have all the write locks
    dprint("srv(%d): done acquiring silo write locks\n", s);

    assert(nlocks == wt_idx);

    // validate rows in the read set now
    uint64_t max_tid = 0;
    for (int i = 0; i < rd_idx; i++) {
        struct op_ctx *octx = &ctx->op_ctx[read_set[i]];
        if ((r = validate_row(octx, 0)) == TXN_ABORT) {
            dprint("srv(%d): readset validation for row %d "
                    "key %"PRId64" failed\n", s, read_set[i], octx->e->key);
            goto final;
        }

        if (octx->e_copy->tid > max_tid)
            max_tid = octx->e_copy->tid;
    }

    // check maxtid with rows in write set. rows in write set should 
    // already be validated. verify that.
    for (int i = 0; i < wt_idx; i++) {
        struct op_ctx *octx = &ctx->op_ctx[write_set[i]];

        if ((r = validate_row(octx, 1)) == TXN_ABORT) {
            dprint("srv(%d): writeset validation for row %d "
                    "key %"PRId64" failed\n", s, read_set[i], octx->e->key);
            goto final;
        }

        if (octx->e_copy->tid > max_tid)
            max_tid = octx->e_copy->tid;
    }

    if (max_tid > p->cur_tid)
        p->cur_tid = max_tid;
    else
        p->cur_tid++;

final:
    if (r == TXN_ABORT) {
        dprint("srv(%d): silo aborting txn\n", s);

        p->naborts_local++;

        for (int i = 0; i < nlocks; i++) {
            struct op_ctx *octx = &ctx->op_ctx[write_set[i]];
            silo_latch_release(octx->e);
        }
    } else {
        dprint("srv(%d): silo commiting txn\n", s);

        // everything was successful. Now write back updates to actual records
        // and update the tid, release latch
        for (int i = 0; i < wt_idx; i++) {
            struct op_ctx *octx = &ctx->op_ctx[write_set[i]];
            memcpy(octx->e->value, octx->e_copy->value, octx->e->size);

#if SILO_USE_ATOMICS
            octx->e->tid = p->cur_tid | SILO_LOCK_BIT;
#else
            octx->e->tid = p->cur_tid;
#endif

            silo_latch_release(octx->e);
        }
    }

    return r;
}

#endif //IF_ENABLE_SILO
