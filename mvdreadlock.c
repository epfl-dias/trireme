#include "headers.h"
#include "smphashtable.h"
#include "plmalloc.h"

#if ENABLE_MVDREADLOCK_CC

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

        if ((e->owners[index] == owner || e->writer == owner) && deps[me]) {
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

int certify_write(struct partition *p, struct elem *e, int s)
{
    char cycle_found = 0;
    int nlocks = 0;

    assert(e->writer == s);

    /* spin on each lock until we get it or we detect a deadlock */
    for (int i = 0; i < NCORES; i++) {
        if (i == s) {
            assert(e->owners[i] == s);
            nlocks++;
            continue;
        }

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

    /* if we hit a cycle, we need to release already acquired locks */
    if (cycle_found) {
        for (int i = nlocks - 1; i >= 0; i--) {
            if (i != s)
                e->owners[i] = -1;
        }
    }

    p->waiting_for = -1;

    return cycle_found ? LOCK_ABORT : LOCK_SUCCESS;
}

void clear_lock(char optype, struct elem *e, int s, char is_certified)
{
    switch(optype) {
    case OPTYPE_LOOKUP:
        assert(e->owners[s] == s);
        e->owners[s] = -1;
        break;

    case OPTYPE_UPDATE:
        assert(e->writer == s);
        assert(e->owners[s] == s);

        if (is_certified) {
            for (int j = NCORES - 1; j >= 0; j--) {
                assert(e->owners[j] == s);
                e->owners[j] = -1;
            }
        }

        e->owners[s] = -1;
        e->writer = -1;

        break;
    }        
}

struct elem *mvdreadlock_acquire(struct partition *p, struct elem *e,
        char optype)
{
    struct elem *target = NULL;
    int s = p - &hash_table->partitions[0];
    int g_tid = p->current_task->g_tid;
    char cycle_found = 0;

    dprint("srv(%d): %s lock request for key %"PRIu64"\n", s,
            optype == OPTYPE_LOOKUP ? "lookup":"update", e->key);

    /* try to get a read lock. If we can get it, good. If not, we just
     * spin until we detect a cycle or we get the read lock
     */
    do {

        int64_t owner;

        if (optype == OPTYPE_LOOKUP)
            owner = __sync_val_compare_and_swap(&e->owners[s], -1, s);
        else
            owner = __sync_val_compare_and_swap(&e->writer, -1, s);

        if (owner == -1) 
            break;

        /* if the owner is blocked, we're blocked on owner */
        p->waiting_for = owner;

        if (cycle_check(e, s, s, owner)) {
            cycle_found = 1;
            break;
        }

    } while (1);

    /* we are no longer blocked */
    p->waiting_for = -1;

    /* if we didnt find cycle, we're good */
    if (!cycle_found) {
        if (optype == OPTYPE_LOOKUP) {
            assert(e->owners[s] == s);
        } else {
            assert(e->writer == s && e->owners[s] == -1);
            e->owners[s] = s;
        }

        target = e;
    }

    return target;
}

void mvdreadlock_abort(struct task *ctask, struct hash_table *hash_table, int s)
{
    struct txn_ctx *ctx = &ctask->txn_ctx;
    struct partition *p = &hash_table->partitions[s];
    int nops = ctx->nops;

    for (int i = 0; i < nops; i++) {
        struct op_ctx *octx = &ctx->op_ctx[i];
        clear_lock(octx->optype, octx->e, s, 0 /*is_certified = 0*/);
    }
}

int mvdreadlock_validate(struct task *ctask, struct hash_table *hash_table, int s)
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
            if ((r = certify_write(p, ctx->op_ctx[i].e, s)) !=
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
        clear_lock(octx->optype, octx->e, s, i <= last_certified_wt);
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

#endif //ENABLE_SVDREADLOCK
