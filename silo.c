#include "headers.h"
#include "smphashtable.h"
#include "benchmark.h"
#include "silo.h"
#include "plmalloc.h"
#include "twopl.h"

#if ENABLE_SILO_CC

void silo_latch_acquire(int s, struct elem *e)
{
    struct partition *p = &hash_table->partitions[s];
    struct lock_entry *l = NULL;

#if SILO_USE_ATOMICS

#if defined(SHARED_EVERYTHING) || defined(SHARED_NOTHING)

    //should not be called in shared everything or nothing mode
    assert(0);

#else

    int r = bwait_acquire(s, p, s /* client id */, p->current_task->g_tid,
            0 /* opid */, e, OPTYPE_UPDATE, &l);

    // in messaging mode, we will be the only thread acquiring a latch on the
    // record. So we can simply set the bit without any synch. If someone else
    // has already set the bit, we can simply yield effectively waiting for it
    // to be released
    while(!l->ready) {
        assert(e->tid & SILO_LOCK_BIT);
        task_yield(&hash_table->partitions[s], TASK_STATE_READY);
    }

    e->tid |= SILO_LOCK_BIT;

#endif //IF_SHARED_EVERYTHING

#else

    LATCH_ACQUIRE(&e->latch, NULL);

    e->tid |= SILO_LOCK_BIT;

#endif
}

void silo_latch_release(int s, struct elem *e)
{
    struct partition *p = &hash_table->partitions[s];

    if (!(e->tid & SILO_LOCK_BIT)) {
        printf("srv(%d):lock bit not set for key %"PRIu64" tid is %"PRIu64"\n", s, e->key, e->tid);
        assert(0);
    }

    assert(e->tid & SILO_LOCK_BIT);

#if defined(SHARED_EVERYTHING)
#if SILO_USE_ATOMICS

    e->tid = e->tid & (~SILO_LOCK_BIT);
#else
    LATCH_RELEASE(&e->latch, NULL);
#endif //ATOMICS

#else // message passing case

    bwait_release(s, p, s, p->current_task->g_tid, 0, e);

    // bwait will clear the lock entry for previous owner. if there is a
    // waiter who is waiting, it becomes the new owner. so its lock entry
    // moves from the waiters list to the owners list. in this case, the
    // tid lock bit must remain set. but if there is no owner, we need to
    // clear the tid bit
    if (!(e->ref_count & DATA_READY_MASK))
        e->tid = e->tid & (~SILO_LOCK_BIT);

#endif
}

static int silo_latch_tryacquire(struct elem *e)
{
#if SILO_USE_ATOMICS
    uint64_t tid = e->tid;
    if (tid & SILO_LOCK_BIT)
        return 0;

    return __sync_bool_compare_and_swap(&e->tid, tid, (tid | SILO_LOCK_BIT));
#else
    return !LATCH_TRY_ACQUIRE(&e->latch);
#endif
}

static int validate_row(int s, struct op_ctx *ctx, char is_write)
{
    /* If write, we already have a lock. so all we need to do is simply check
     * if the tid has changed. 
     * If read, we check if someone else has a lock. If so, abort. If not,
     * check the tid
     */
    int r;

#if SILO_USE_ATOMICS
    uint64_t copy_tid = ctx->tid_copy, tid = ctx->e->tid;
    if (is_write)
        return ((copy_tid & (~SILO_LOCK_BIT)) == (tid & (~SILO_LOCK_BIT)) ? 
                TXN_COMMIT : TXN_ABORT);

    if (tid & SILO_LOCK_BIT)
        return TXN_ABORT;
    else if ((copy_tid & (~SILO_LOCK_BIT)) != (tid & (~SILO_LOCK_BIT)))
        return TXN_ABORT;
    else
        return TXN_COMMIT;

#else
    if (!is_write) {
        if (!silo_latch_tryacquire(ctx->e))
            return TXN_ABORT;
    }

    if (ctx->e->tid == ctx->tid_copy)
        r = TXN_COMMIT;
    else
        r = TXN_ABORT;

    if (!is_write) {
        silo_latch_release(s, ctx->e);
    }
#endif

    return r;
}

static int preabort_check(int s, struct txn_ctx *ctx, int *write_set,
        int wt_idx, int *read_set, int rd_idx)
{
    for (int i = 0; i < wt_idx; i++) {
        struct op_ctx *octx = &ctx->op_ctx[write_set[i]];
        if (octx->e->tid != octx->tid_copy) {
            dprint("srv(%d): preabort check failed on write op %d "
                    "key %"PRId64" tid %d copy tid %d \n", s, write_set[i],
                    octx->e->key, octx->e->tid, octx->tid_copy);

            return TXN_ABORT;
        }
    }

    for (int i = 0; i < rd_idx; i++) {
        struct op_ctx *octx = &ctx->op_ctx[read_set[i]];
        if (octx->e->tid != octx->tid_copy) {
            dprint("srv(%d): preabort check failed on read op %d "
                    "key %"PRId64" tid %d copy tid %d \n", s, read_set[i],
                    octx->e->key, octx->e->tid, octx->tid_copy);

            return TXN_ABORT;
        }
    }

    return TXN_COMMIT;
}

// code for silo_validate in shared everything case
int silo_validate(struct task *ctask, struct hash_table *hash_table, int s)
{
    struct txn_ctx *ctx = &ctask->txn_ctx;
    struct partition *p = &hash_table->partitions[s];
    int nops = ctx->nops;
    int r = TXN_COMMIT;
    int nlocks = 0;

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

#if SHARED_EVERYTHING
    /* lock all rows in the write set now */
    char done_locking = 0;
    while (!done_locking) {
        nlocks = 0;
        for (int i = 0; i < wt_idx; i++) {
            struct op_ctx *octx = &ctx->op_ctx[write_set[i]];
            if (!silo_latch_tryacquire(octx->e))
                break;

            nlocks++;
            if ((octx->e->tid & (~SILO_LOCK_BIT)) != octx->tid_copy) {
                dprint("srv(%d): abort due to tid mismatch %"PRIu64
                        "--%"PRIu64" at %d\n", s, octx->e->tid, 
                        octx->tid_copy, i);

                r = TXN_ABORT;
                goto final;
            }
        }

        if (nlocks == wt_idx) {
            done_locking = 1;
        } else {

            // we were not able to get all locks. release, sleep and repeat
            if ((r = preabort_check(s, ctx, write_set, wt_idx, read_set,
                            rd_idx)) == TXN_ABORT) {
                dprint("srv(%d): abort due to preabort while retrying locks\n", s);
                goto final;
            } else {
                for (int i = 0; i < nlocks; i++) {
                    struct op_ctx *octx = &ctx->op_ctx[write_set[i]];

#if SILO_USE_ATOMICS
                    assert(octx->e->tid & SILO_LOCK_BIT);
#endif

                    silo_latch_release(s, octx->e);
                }
            }
        }
    }
#else

    // don't bother with non-blocking version. just acquire lock one at a time
    // and be done. we can't deadlock here as we are locking in key order
    nlocks = 0;
    for (int i = 0; i < wt_idx; i++) {
        struct op_ctx *octx = &ctx->op_ctx[write_set[i]];
        if (octx->target == s) {
            // if it is local, simply acquire the latch
           silo_latch_acquire(s, octx->e);
        } else {
            // if it is remote, send a message asking for the latch
            // XXX: Here, we send one at a time, we can batch it all and send it
            // together in one shot. This will optimize things further.
            struct hash_op op, *pop[1];
            pop[0] = &op;
            op.optype = OPTYPE_UPDATE;
            op.size = 0;
            op.key = octx->e->key;

            void *tmp, *ptmp[1];
            ptmp[0] = tmp;
            smp_hash_doall(ctask, hash_table, s, octx->target, 1, pop, ptmp, 0);

            // the lock better be set
            assert(octx->e->tid & SILO_LOCK_BIT);
        }

        nlocks++;

        if ((octx->e->tid & (~SILO_LOCK_BIT)) != octx->tid_copy) {
            dprint("srv(%d): abort due to tid mismatch %"PRIu64
                    "--%"PRIu64" at %d\n", s, octx->e->tid, 
                    octx->tid_copy, i);

            r = TXN_ABORT;
            goto final;
        }
    }

    assert(nlocks == wt_idx);

#endif

    // at this point we should have all the write locks
    dprint("srv(%d): done acquiring silo write locks\n", s);

    assert(nlocks == wt_idx);

    // validate rows in the read set now
    uint64_t max_tid = 0;
    for (int i = 0; i < rd_idx; i++) {
        struct op_ctx *octx = &ctx->op_ctx[read_set[i]];
        if ((r = validate_row(s, octx, 0)) == TXN_ABORT) {
            dprint("srv(%d): readset validation for row %d "
                    "key %"PRId64" failed\n", s, read_set[i], octx->e->key);
            goto final;
        }

        if (octx->tid_copy > max_tid)
            max_tid = octx->tid_copy;
    }

    // check maxtid with rows in write set. rows in write set should 
    // already be validated. verify that.
    for (int i = 0; i < wt_idx; i++) {
        struct op_ctx *octx = &ctx->op_ctx[write_set[i]];

        if ((r = validate_row(s, octx, 1)) == TXN_ABORT) {
            dprint("srv(%d): writeset validation for row %d "
                    "key %"PRId64" failed\n", s, read_set[i], octx->e->key);
            goto final;
        }

        if (octx->tid_copy > max_tid)
            max_tid = octx->tid_copy;
    }

    if (max_tid > p->cur_tid)
        p->cur_tid = max_tid + 1;
    else
        p->cur_tid++;

final:
    if (r == TXN_ABORT) {
        dprint("srv(%d): silo aborting txn\n", s);

        for (int i = 0; i < nlocks; i++) {
            struct op_ctx *octx = &ctx->op_ctx[write_set[i]];
#if SILO_USE_ATOMICS
            assert (octx->e->tid & SILO_LOCK_BIT);
#endif

#if SHARED_EVERYTHING
            // in shared everything, we can simply release
            silo_latch_release(s, octx->e);
#else
            // in message passing mode, we need to directly release if local or
            // send a release message if remote
            if (octx->target == s) {
                silo_latch_release(s, octx->e);
            } else {
                mp_mark_ready(hash_table, s, octx->target, ctask->tid, 0, octx->e);
            }
#endif 
        }
    } else {
        dprint("srv(%d): silo commiting txn\n", s);

        // everything was successful. Now write back updates to actual records
        // and update the tid, release latch
        for (int i = 0; i < wt_idx; i++) {
            struct op_ctx *octx = &ctx->op_ctx[write_set[i]];

            memcpy(octx->e->value, octx->data_copy->value, octx->e->size);

            COMPILER_BARRIER();

#if SILO_USE_ATOMICS
            assert(octx->e->tid & SILO_LOCK_BIT);
            assert(((octx->e->tid & (~SILO_LOCK_BIT)) != p->cur_tid));
            octx->e->tid = p->cur_tid | SILO_LOCK_BIT;
#else
            octx->e->tid = p->cur_tid;
#endif

#if SHARED_EVERYTHING
            silo_latch_release(s, octx->e);
#else
            // in message passing mode, we need to directly release if local or
            // send a release message if remote
            if (octx->target == s) {
                silo_latch_release(s, octx->e);
            } else {
                mp_mark_ready(hash_table, s, octx->target, ctask->tid, 0, octx->e);
            }
#endif 
        }
    }

    return r;
}
#endif //IF_ENABLE_SILO
