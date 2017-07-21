#include "headers.h"
#include "smphashtable.h"
#include "benchmark.h"
#include "twopl.h"
#include "plmalloc.h"
//#include "dl_detect.h"
#include "se_dl_detect_graph.h"
#include "dreadlock_detect.h"

//#if SHARED_EVERYTHING

#if ENABLE_WAIT_DIE_CC
int selock_wait_die_acquire(struct partition *p, struct elem *e, 
    char optype, uint64_t req_ts)
{
    struct lock_entry *target, *l;
    int s = p - &hash_table->partitions[0];
    int g_tid = p->current_task->g_tid;

    dprint("srv(%d-%"PRIu64"): %s lock request for key %"PRIu64"\n", s, req_ts,
            optype == OPTYPE_LOOKUP ? "lookup":"update", e->key);

    /* latch the record. check to see if it is a conflicting lock mode
     * if so, bad luck. we just fail
     */
    int r = 0;
    char wait = 0;
    int alock_state;

#if SE_LATCH
    LATCH_ACQUIRE(&e->latch, &alock_state);

    // there is a conflict if e's refcount indicates an exclusive lock
    char conflict = !is_value_ready(e);

#if VERIFY_CONSISTENCY
    //if conflict, there must be one and exactly one owner
    if (conflict) {
        int nowners = 0;
        LIST_FOREACH(l, &e->owners, next) {
            nowners++;
        }

        assert(nowners == 1);
    }
#endif

    // even if no exclusive lock, conflict if we want an update when
    // there are read locks
    if (!conflict && (optype == OPTYPE_UPDATE || optype == OPTYPE_INSERT))
        conflict = (e->ref_count > 1);

    /* with wait die, we also have a conflict if there is a waiting list
     * and if head of waiting list is a newer txn than incoming one
     */
    if (!conflict) {
        if (!LIST_EMPTY(&e->waiters)) {
            l = LIST_FIRST(&e->waiters);
            if (req_ts <= l->ts)
                conflict = 1;
        }
    }

    /* if there are no conflicts, we reset refcount */
    if (!conflict) {
        dprint("srv(%d-%"PRIu64"): %s lock request for key %"PRIu64" granted w/o conflict\n", 
                s, req_ts, optype == OPTYPE_LOOKUP ? "lookup":"update", e->key);

        if (optype == OPTYPE_LOOKUP) {
            e->ref_count++;
            r = 1;
        } else {
            e->ref_count = DATA_READY_MASK | 2;
            r = 1;
        }

        // insert a lock in the owners list
        target = plmalloc_alloc(p, sizeof(struct lock_entry));
        assert(target);
        target->ts = req_ts;
        target->s = s;
        target->task_id = g_tid;
        target->optype = optype;
        target->ready = 1;

        LIST_INSERT_HEAD(&e->owners, target, next);

    } else {

        /* There was a conflict. In wait die case, we can wait if req_ts is < ts 
         * of all owner txns 
         */
        wait = 1;
        LIST_FOREACH(l, &e->owners, next) {
            if (l->ts < req_ts || ((l->ts == req_ts) && (l->task_id < g_tid))) {
                wait = 0;
                break;
            }
        }

        if (wait) {
            dprint("srv(%d-%"PRIu64"): %s lock request for key %"PRIu64" put under wait\n", 
                    s, req_ts, optype == OPTYPE_LOOKUP ? "lookup":"update", e->key);

            // if we are allowed to wait, make a new lock entry and add it to
            // waiters list
            struct lock_entry *last_lock = NULL;
            LIST_FOREACH(l, &e->waiters, next) {
                if (l->ts < req_ts || (l->ts == req_ts && l->task_id < g_tid))
                    break;

                last_lock = l;
            }

            target = plmalloc_alloc(p, sizeof(struct lock_entry));
            assert(target);
            target->ts = req_ts;
            target->s = s;
            target->task_id = g_tid;
            target->optype = optype;
            target->ready = 0;
            if (l) {
                LIST_INSERT_BEFORE(l, target, next);
            } else {
                if (last_lock) {
                    LIST_INSERT_AFTER(last_lock, target, next);
                } else {
                    LIST_INSERT_HEAD(&e->waiters, target, next);
                }
            }
        } else {
            dprint("srv(%d-%"PRIu64"): %s lock request for key %"PRIu64" aborting\n", 
                    s, req_ts, optype == OPTYPE_LOOKUP ? "lookup":"update", e->key);
        }
    }

    LATCH_RELEASE(&e->latch, &alock_state);

    if (wait) {
        assert(r == 0);

        /* now spin until another thread signals us that we have the lock */
        assert(target);

        dprint("srv(%d-%"PRIu64"): %s lock request for key %"PRIu64" spinning until ready\n", 
                s, req_ts, optype == OPTYPE_LOOKUP ? "lookup":"update", e->key);

#if defined(MIGRATION)
        while (!target->ready) {
            task_yield(p, TASK_STATE_READY);
        }
#else
        while (!target->ready) ;
#endif

        dprint("srv(%d-%"PRIu64"): %s lock request for key %"PRIu64" stopped spinning\n", 
                s, req_ts, optype == OPTYPE_LOOKUP ? "lookup":"update", e->key);

        if (optype == OPTYPE_LOOKUP) {
            assert(e->ref_count > 1);
        } else {
            assert((e->ref_count & DATA_READY_MASK) && 
                    ((e->ref_count & ~DATA_READY_MASK) == 2));
        }

        r = 1;
    }

#else
    assert(g_benchmark == &micro_bench && g_write_threshold == 100);
    r = 1;
#endif // SE_LATCH

    return r;

}

void selock_wait_die_release(struct partition *p, struct op_ctx *octx)
{
    /* latch, reset ref count to free the logical lock, unlatch */
    int alock_state;
    int s = p - &hash_table->partitions[0];
    int g_tid = p->current_task->g_tid;
    struct elem *e = octx->e;

#if SE_LATCH
    LATCH_ACQUIRE(&e->latch, &alock_state);

    dprint("srv(%d): releasing lock request for key %"PRIu64"\n", s, e->key);

    /* go through the owners list and remove the txn */
    struct lock_entry *lock_entry;

    LIST_FOREACH(lock_entry, &e->owners, next) {
        if (lock_entry->task_id == g_tid)
            break;
    }

    if (!lock_entry) {
        printf("srv(%d): FAILED releasing lock request for key %"PRIu64"\n", s, e->key);
    }

    assert(lock_entry);

    LIST_REMOVE(lock_entry, next);

    // decrement ref count as a owner is releasing a lock
    e->ref_count = (e->ref_count & ~DATA_READY_MASK) - 1;

    plmalloc_free(p, lock_entry, sizeof(*lock_entry));

    // if there are no more owners, then refcount should be 1
    if (LIST_EMPTY(&e->owners))
        assert(e->ref_count == 1);

    /* If lock_free is set, that means the new lock mode is decided by 
     * the head of waiter list. If lock_free is not set, we still have
     * some readers. So only pending readers can be allowed. Keep 
     * popping items from wait list as long as we have readers.
     */
    lock_entry = LIST_FIRST(&e->waiters);
    while (lock_entry) {
        char conflict = 0;

        dprint("srv(%d): release request for key %"PRIu64" found %d waiting\n", 
                s, e->key, lock_entry->s);

        if (lock_entry->optype == OPTYPE_LOOKUP) {
            conflict = !is_value_ready(e);
        } else {
            assert(lock_entry->optype == OPTYPE_UPDATE);
            conflict = (!is_value_ready(e)) || ((e->ref_count & ~DATA_READY_MASK) > 1);
        }

        if (conflict) {
            dprint("srv(%d): release request for key %"PRIu64" found %d in conflict"
                    "ref count was %"PRIu64"\n", s, e->key, lock_entry->s, e->ref_count);
            break;

        } else {
            /* there's no conflict only if there is a shared lock and we're 
             * requesting a shared lock, or if there's no lock
             */
            assert((e->ref_count & DATA_READY_MASK) == 0);

            if (lock_entry->optype == OPTYPE_LOOKUP) {
                assert((e->ref_count & ~DATA_READY_MASK) >= 1);
                e->ref_count++;
            } else {
                assert((e->ref_count & ~DATA_READY_MASK) == 1);
                e->ref_count = DATA_READY_MASK | 2;
            }
        }

        // remove from waiters, add to owners, mark as ready
        LIST_REMOVE(lock_entry, next);
        LIST_INSERT_HEAD(&e->owners, lock_entry, next);
        lock_entry->ready = 1;

        dprint("srv(%d): release lock request for key %"PRIu64" marking %d as ready\n", 
                s, e->key, lock_entry->s);

        // go to next element
        lock_entry = LIST_FIRST(&e->waiters);
    }

    LATCH_RELEASE(&e->latch, &alock_state);

#else
    assert(g_benchmark == &micro_bench && g_write_threshold == 100);
#endif // SE_LATCH

}
#endif // ENABLE_WAIT_DIE_CC

#if ENABLE_NOWAIT_OWNER_CC
/*
 * NOWAIT implementation that maintains an owner list
 */
int selock_nowait_with_ownerlist_acquire(struct partition *p, struct elem *e,
        char optype, uint64_t req_ts)
{
    int r = 0, alock_state;

    LATCH_ACQUIRE(&e->latch, &alock_state);

    // there is a conflict if e's refcount indicates an exclusive lock
    char conflict = !is_value_ready(e);

    // even if no exclusive lock, conflict if we want an update when
    // there are read locks
    if (!conflict && (optype == OPTYPE_UPDATE || optype == OPTYPE_INSERT))
        conflict = e->ref_count > 1;

    /* if there are no conflicts  locks, we reset refcount */
    if (!conflict) {
        if (optype == OPTYPE_LOOKUP) {
            e->ref_count++;
            r = 1;
        } else {
            e->ref_count = DATA_READY_MASK | 2;
            r = 1;
        }
    }

    if (r) {
        // success. add to owner list
        struct lock_entry *target  = plmalloc_alloc(p, sizeof(struct lock_entry));
        assert(target);
        target->s = p - hash_table->partitions; 
        target->task_id = p->current_task->g_tid;
        target->optype = optype;
        target->ready = 1;

        LIST_INSERT_HEAD(&e->owners, target, next);
    }

    LATCH_RELEASE(&e->latch, &alock_state);

    return r;
}

void selock_nowait_with_ownerlist_release(struct partition *p, struct op_ctx *ctx)
{
    struct lock_entry *lock_entry;
    int s = p - hash_table->partitions;
    int g_tid = p->current_task->g_tid;
    int alock_state;
    struct elem *e = octx->e;

    LATCH_ACQUIRE(&e->latch, &alock_state);

    // remove from owner list
    LIST_FOREACH(lock_entry, &e->owners, next) {
        if (lock_entry->task_id == g_tid)
            break;
    }

    assert(lock_entry);

    LIST_REMOVE(lock_entry, next);

    plmalloc_free(p, lock_entry, sizeof(*lock_entry));

    e->ref_count = (e->ref_count & (~DATA_READY_MASK)) - 1;

    LATCH_RELEASE(&e->latch, &alock_state);

}
#endif //ENABLE_NOWAIT_OWNER_CC

#if ENABLE_NOWAIT_CC
/*
 * NOWAIT implementation that does not maintain any lists explicitly
 */
int selock_nowait_acquire(struct partition *p, struct elem *e, char optype, 
    uint64_t req_ts)
{
    /* latch the record. check to see if it is a conflicting lock mode
     * if so, bad luck. we just fail
     */
    int r = 0;
    int alock_state;

#if SE_LATCH

#if RWTICKET_LOCK
    int nretries = 10;
    while (nretries-- && !r) {
        if (optype == OPTYPE_LOOKUP) {
            r = rwticket_rdtrylock(&e->latch);
        }  else {
            r = rwticket_wrtrylock(&e->latch);
        }
    }

#elif RW_LOCK
    if (optype == OPTYPE_LOOKUP) {
        r = rwlock_rdtrylock(&e->latch);
    }  else {
        r = rwlock_wrtrylock(&e->latch);
    }
#elif DRW_LOCK
    if (optype == OPTYPE_LOOKUP) {
        r = drwlock_rdtrylock(p - &(hash_table->partitions[0]), &e->latch);
    }  else {
        r = drwlock_wrtrylock(&e->latch);
    }

#else
    LATCH_ACQUIRE(&e->latch, &alock_state);

    // there is a conflict if e's refcount indicates an exclusive lock
    char conflict = !is_value_ready(e);

    // even if no exclusive lock, conflict if we want an update when
    // there are read locks
    if (!conflict && (optype == OPTYPE_UPDATE || optype == OPTYPE_INSERT))
        conflict = e->ref_count > 1;

    /* if there are no conflicts  locks, we reset refcount */
    if (!conflict) {
        if (optype == OPTYPE_LOOKUP) {
            e->ref_count++;
            r = 1;
        } else {
            e->ref_count = DATA_READY_MASK | 2;
            r = 1;
        }
    }

    LATCH_RELEASE(&e->latch, &alock_state);
#endif //RWTICKET_LOCK

#else
    // only time latching is disabled is when we are doing ronly bench
    assert(g_benchmark == &micro_bench && g_write_threshold == 100);
    r = 1;
#endif //SE_LATCH

    return r;
}

void selock_nowait_release(struct partition *p, struct op_ctx *octx)
{
    /* latch, reset ref count to free the logical lock, unlatch */
    int alock_state;
    struct elem *e = octx->e;
    char optype = octx->optype;

#if SE_LATCH
#if RWTICKET_LOCK
    if (optype == OPTYPE_LOOKUP)
        rwticket_rdunlock(&e->latch);
    else
        rwticket_wrunlock(&e->latch);

#elif RW_LOCK
    if (optype == OPTYPE_LOOKUP)
        rwlock_rdunlock(&e->latch);
    else
        rwlock_wrunlock(&e->latch);

#elif DRW_LOCK
    if (optype == OPTYPE_LOOKUP)
        drwlock_rdunlock(p - &(hash_table->partitions[0]), &e->latch);
    else
        drwlock_wrunlock(&e->latch);

#else
    LATCH_ACQUIRE(&e->latch, &alock_state);

    e->ref_count = (e->ref_count & (~DATA_READY_MASK)) - 1;

    LATCH_RELEASE(&e->latch, &alock_state);
#endif // RWTICKET_LOCK
#endif //SE_LATCH

}
#endif//IF_ENABLE_NOWAIT

#if ENABLE_DL_DETECT_CC
#include "glo.h"
#if ENABLE_CYCLE_DETECTION
int selock_dl_detect_acquire(struct partition *p, struct elem *e,
		char optype, uint64_t req_ts)
{
    dprint("DL_DETECT\n");

    struct lock_entry *target, *l;
    int s = p - &hash_table->partitions[0];
    int g_tid = p->current_task->g_tid;

    int r = 0;
    char wait = 0;

    dprint("srv(%d-%"PRIu64"): %s lock request for key %"PRIu64"\n", s, req_ts,
            optype == OPTYPE_LOOKUP ? "lookup":"update", e->key);

    int alock_state;
    uint64_t *txnids;
    int txncnt = 0;

#if SE_LATCH
    LATCH_ACQUIRE(&e->latch, &alock_state);
    char conflict = !is_value_ready(e);

#if VERIFY_CONSISTENCY
    if (conflict) {
        int nowners = 0;
        TAILQ_FOREACH(l, &e->owners, next) {
            nowners ++;
        }
    }

    assert(nowners == 1);
#endif //VERIFY_CONSISTENCY

    if (!conflict && (optype == OPTYPE_UPDATE || optype == OPTYPE_INSERT)) {
        conflict = e->ref_count > 1;
    }

    if (!conflict) {
        if (!TAILQ_EMPTY(&e->waiters)) {
            conflict = 1;
        }
    }

    bool lock_abort = false;
    if (!conflict) {
        uint64_t srv = g_tid;
        dprint("srv(%d-%"PRIu64"): %s lock request for key %"PRIu64" granted w/o conflict\n",
                s, req_ts, optype == OPTYPE_LOOKUP ? "lookup":"update", e->key);

        if (optype == OPTYPE_LOOKUP) {
            e->ref_count++;
            r = 1;
        } else {
            e->ref_count = DATA_READY_MASK | 2;
            r = 1;
        }

        // insert a lock in the owners list
        target = plmalloc_alloc(p, sizeof(struct lock_entry));
        assert(target);
        target->ts = req_ts;
        target->s = s;
        target->task_id = g_tid;
        target->optype = optype;
        target->ready = 1;

        TAILQ_INSERT_HEAD(&e->owners, target, next);

        if (!TAILQ_EMPTY(&e->waiters)) {
            struct se_dl_detect_graph_node src;
            struct se_waiter_node dst;
            dst.srvfib = g_tid;
            dst.opid = 0;
            dst.ts = req_ts;
            src.neighbors = &dst;
            src.waiters_size = 1;
            src.e = e;
            src.opid = 0;
            TAILQ_FOREACH(l, &e->waiters, next) {
                src.srvfib = l->task_id;
                src.sender_srv = s;
                src.ts = l->ts;
                se_dl_detect_add_dependency(&src);
            }
        }

    } else {
        dprint("srv(%d-%"PRIu64"): There is conflict!!\n", s, req_ts);
        uint64_t starttime = get_sys_clock();
        bool dep_added = false;
        int owners_cnt = 0;
        int waiters_cnt = 0;
        TAILQ_FOREACH(l, &e->owners, next) {
            owners_cnt ++;
        }
        TAILQ_FOREACH(l, &e->waiters, next) {
            waiters_cnt ++;
        }
        txncnt = owners_cnt + waiters_cnt;
        wait = 1;
        uint64_t txnid = g_tid;
        if (txncnt) {
        	// TODO: Replaced here
            struct se_dl_detect_graph_node src;
            src.neighbors = plmalloc_alloc(p, txncnt * sizeof(struct se_waiter_node));
            src.e = e;
            src.srvfib = g_tid;
            src.opid = 0;
            src.sender_srv = s;
            src.ts = req_ts;
            src.waiters_size = txncnt;

            int cur = 0;
            TAILQ_FOREACH(l, &e->owners, next) {
                src.neighbors[cur].opid = 0;
                src.neighbors[cur].srvfib = l->task_id;
                src.neighbors[cur].ts = l->ts;
                cur++;
            }
            TAILQ_FOREACH(l, &e->waiters, next) {
                src.neighbors[cur].opid = 0;
                src.neighbors[cur].srvfib = l->task_id;
                src.neighbors[cur].ts = l->ts;
                cur++;
            }
            int added = se_dl_detect_add_dependency(&src);
            if (added) {
                int deadlock_flag = 0;
                int cycle = se_dl_detect_detect_cycle(g_tid, &src);
                if (cycle) {
                    wait = 0;
                    se_dl_detect_clear_dependencies(&src, 1);
                }
            }

            plmalloc_free(p, src.neighbors, txncnt * sizeof(struct se_waiter_node));
        }

#if 0
        while (!lock_abort) {
            uint64_t last_detect = starttime;
            uint64_t last_try = starttime;

            uint64_t now = get_sys_clock();
            if (now - starttime > DL_DETECT_TIMEOUT ) {
                wait = 0;
                lock_abort = true;
                break;
            }

            int ok = 0;
            if ((now - last_detect > DL_LOOP_DETECT) && (now - last_try > DL_LOOP_TRIAL)) {
                dprint("srv(%d-%"PRIu64"): Checking for deadlock\n", s, req_ts);
                if (!dep_added) {
                    // TODO we need the txn id; will use the server id but we need to check
                    // we assume that each txn requests 1 elements and thus holds 0 locks so far
                    ok = DL_detect_add_dep(p, &dl_detector, txnid, txnids, txncnt, 1);
                    if (ok == 0)
                        dep_added = true;
                    else if (ok == 16)
                        last_try = now;
                }
                if (dep_added) {
                    dprint("srv(%d-%"PRIu64"): Added the dependency\n", s, req_ts);
                    ok = DL_detect_detect_cycle(p, &dl_detector, txnid);
                    if (ok == 16)  // failed to lock the deadlock detector
                        last_try = now;
                    else if (ok == 0) {
                        last_detect = now;
                        wait = 1;
                        lock_abort = true;
                        dprint("srv(%d-%"PRIu64"): No DEADLOCK!!!\n", s, req_ts);
                    }
                    else if (ok == 1) {
                        last_detect = now;
                        wait = 0; // we have a deadlock
                        lock_abort = true;
                        dprint("srv(%d-%"PRIu64"): DEADLOCK\n", s, req_ts);
                        r = 0;
                    }
                }
            } else
                PAUSE
        }
#endif
        if (wait) {
            dprint("srv(%d-%"PRIu64"): %s lock request for key %"PRIu64" put under wait\n",
                    s, req_ts, optype == OPTYPE_LOOKUP ? "lookup":"update", e->key);

            // if we are allowed to wait, make a new lock entry and add it to
            // waiters list
            struct lock_entry *last_lock = NULL;
            TAILQ_FOREACH(l, &e->owners, next) {
                dprint("srv(%d-%"PRIu64"): Waiting for owner %d\n", s, req_ts, l->task_id);
            }
            TAILQ_FOREACH(l, &e->waiters, next) {
                dprint("srv(%d-%"PRIu64"): Waiting for waiter %d\n", s, req_ts, l->task_id);
                last_lock = l;
            }

            target = plmalloc_alloc(p, sizeof(struct lock_entry));
            assert(target);
            target->ts = req_ts;
            target->s = s;
            target->task_id = g_tid;
            target->optype = optype;
            target->ready = 0;
            //          if (last_lock) {
            //              TAILQ_INSERT_AFTER(&e->waiters, last_lock, target, next);
            //          } else {
            TAILQ_INSERT_HEAD(&e->waiters, target, next);
            //          }

            dprint("srv(%d-%"PRIu64"): DONE Preparing waiter list\n", s, req_ts);
        } else {
            dprint("srv(%d-%"PRIu64"): %s lock request for key %"PRIu64" aborting\n",
                    s, req_ts, optype == OPTYPE_LOOKUP ? "lookup":"update", e->key);
        }

    }

    LATCH_RELEASE(&e->latch, &alock_state);
    dprint("srv(%d-%"PRIu64"): Released the latch and going to wait\n", s, req_ts);

    if (wait) {
        assert(r == 0);

        /* now spin until another thread signals us that we have the lock */
        assert(target);

        dprint("srv(%d-%"PRIu64"): %s lock request for key %"PRIu64" spinning until ready\n",
                s, req_ts, optype == OPTYPE_LOOKUP ? "lookup":"update", e->key);


#if MIGRATION
        while (!target->ready) {
            task_yield(p, TASK_STATE_READY);
        }
#else
        while (!target->ready) ;
#endif

        dprint("srv(%d-%"PRIu64"): %s lock request for key %"PRIu64" stopped spinning\n",
                s, req_ts, optype == OPTYPE_LOOKUP ? "lookup":"update", e->key);

        if (optype == OPTYPE_LOOKUP) {
            assert(e->ref_count > 1);
        } else {
            assert((e->ref_count & DATA_READY_MASK) &&
                    ((e->ref_count & ~DATA_READY_MASK) == 2));
        }

        r = 1;
    }
#else
    assert(g_benchmark == &micro_bench && g_write_threshold == 100);
    r = 1;

#endif //SE_LATCH

    return r;
}

void selock_dl_detect_release(struct partition *p, struct op_ctx *octx)
{
    int alock_state;
    int s = p - &hash_table->partitions[0];
    int g_tid = p->current_task->g_tid;
    struct elem *e = octx->e;

    //	printf("Releasing the lock\n");
    dprint("srv(%d): Releasing lock on key %d\n", s, e->key);

#if SE_LATCH
    dprint("srv(%d): acquiring latch for key %"PRIu64"\n", s, e->key);
    LATCH_ACQUIRE(&e->latch, &alock_state);

    dprint("srv(%d): releasing lock request for key %"PRIu64"\n", s, e->key);

    /* go through the owners list and remove the txn */
    struct lock_entry *lock_entry;

    int found_in_owners = 0;
    TAILQ_FOREACH(lock_entry, &e->owners, next) {
        if (lock_entry->task_id == g_tid) {
            found_in_owners = 1;
            break;
        }
    }

    if (!found_in_owners) {
        TAILQ_FOREACH(lock_entry, &e->owners, next) {
            if (lock_entry->task_id == g_tid) {
                break;
            }
        }
    }

    if (!lock_entry) {
        printf("srv(%d): FAILED releasing lock request for key %"PRIu64"\n", s, e->key);
    }

    assert(lock_entry);

    TAILQ_REMOVE(&e->owners, lock_entry, next);

    // decrement ref count as a owner is releasing a lock
    e->ref_count = (e->ref_count & ~DATA_READY_MASK) - 1;

    plmalloc_free(p, lock_entry, sizeof(*lock_entry));

    // if there are no more owners, then refcount should be 1
    if (TAILQ_EMPTY(&e->owners))
        assert(e->ref_count == 1);

    /* If lock_free is set, that means the new lock mode is decided by
     * the head of waiter list. If lock_free is not set, we still have
     * some readers. So only pending readers can be allowed. Keep
     * popping items from wait list as long as we have readers.
     */
    lock_entry = TAILQ_LAST(&e->waiters, lock_tail);
    while (lock_entry) {
        char conflict = 0;

        dprint("srv(%d): release request for key %"PRIu64" found %d waiting\n",
                s, e->key, lock_entry->task_id);

        if (lock_entry->optype == OPTYPE_LOOKUP) {
            conflict = !is_value_ready(e);
        } else {
            assert(lock_entry->optype == OPTYPE_UPDATE);
            conflict = (!is_value_ready(e)) || ((e->ref_count & ~DATA_READY_MASK) > 1);
        }

        if (conflict) {
            dprint("srv(%d): release request for key %"PRIu64" found %d in conflict "
                    "ref count was %"PRIu64"\n", s, e->key, lock_entry->task_id, e->ref_count);
            break;

        } else {
            /* there's no conflict only if there is a shared lock and we're
             * requesting a shared lock, or if there's no lock
             */
            assert((e->ref_count & DATA_READY_MASK) == 0);

            if (lock_entry->optype == OPTYPE_LOOKUP) {
                assert((e->ref_count & ~DATA_READY_MASK) >= 1);
                e->ref_count++;
            } else {
                assert((e->ref_count & ~DATA_READY_MASK) == 1);
                e->ref_count = DATA_READY_MASK | 2;
            }
        }

        // remove from waiters, add to owners, mark as ready
        TAILQ_REMOVE(&e->waiters, lock_entry, next);
        TAILQ_INSERT_HEAD(&e->owners, lock_entry, next);
        //	DL_detect_clear_dep(p, &dl_detector, (uint64_t) lock_entry->task_id);
        struct se_dl_detect_graph_node src;
        src.e = e;
        src.opid = 0;
        src.sender_srv = s;
        src.srvfib = lock_entry->task_id;
        src.ts = lock_entry->ts;

        se_dl_detect_clear_dependencies(&src, 0);

        //	struct se_waiter_node dst;
        //	src.neighbors = &dst;
        //	dst.opid = 0;
        //	dst.srvfib = lock_entry->task_id;
        //	dst.ts = lock_entry->ts;
        //
        //	src.waiters_size = 1;
        //	struct lock_entry *l;
        //	TAILQ_FOREACH(l, &e->waiters, next) {
        //		src.sender_srv = s;
        //		src.srvfib = l->task_id;
        //		src.ts = l->ts;
        //
        //		se_dl_detect_add_dependency(&src);
        ////		uint64_t srv = lock_entry->task_id;
        ////		DL_detect_add_dep(p, &dl_detector, l->task_id, &srv, 1, 1);
        //	}
        lock_entry->ready = 1;

        dprint("srv(%d): release lock request for key %"PRIu64" marking %d as ready\n",
                s, e->key, lock_entry->task_id);

        // go to next element
        lock_entry = TAILQ_LAST(&e->waiters, lock_tail);
    }

    LATCH_RELEASE(&e->latch, &alock_state);

#else
    assert(g_benchmark == &micro_bench && g_write_threshold == 100);
#endif // SE_LATCH

    /* WAIT DIE */


    dprint("srv(%d): Released lock on key %d\n", s, e->key);
}

#else // ENABLE_CYCLE_DETECTION
int selock_dl_detect_acquire(struct partition *p, struct elem *e,
		char optype, uint64_t req_ts)
{
    dprint("DL_DETECT\n");

    struct lock_entry *target, *l;
    int s = p - &hash_table->partitions[0];
    int g_tid = p->current_task->g_tid;

    int r = 0;
    char wait = 0;

    dprint("srv(%d-%"PRIu64"): %s lock request for key %"PRIu64"\n", s, req_ts,
            optype == OPTYPE_LOOKUP ? "lookup":"update", e->key);

    int alock_state;
    uint64_t *txnids;
    int txncnt = 0;

    int nof_servers = g_nservers * g_batch_size;
	int dep_srv[nof_servers];
	uint64_t dep_tid[nof_servers];
	int all_deps = 0;
	target = plmalloc_alloc(p, sizeof(struct lock_entry));
	assert(target);
	target->ts = req_ts;
	target->s = s;
	target->task_id = g_tid;
	target->optype = optype;

#if SE_LATCH
    LATCH_ACQUIRE(&e->latch, &alock_state);
    char conflict = !is_value_ready(e);

#if VERIFY_CONSISTENCY
    if (conflict) {
        int nowners = 0;
        TAILQ_FOREACH(l, &e->owners, next) {
            nowners ++;
        }
    }

    assert(nowners == 1);
#endif //VERIFY_CONSISTENCY

    if (!conflict && (optype == OPTYPE_UPDATE || optype == OPTYPE_INSERT)) {
        conflict = e->ref_count > 1;
    }

    if (!conflict) {
        if (!TAILQ_EMPTY(&e->waiters)) {
            conflict = 1;
        }
    }

    bool lock_abort = false;

    if (!conflict) {
        uint64_t srv = g_tid;
        dprint("srv(%d-%"PRIu64"): %s lock request for key %"PRIu64" granted w/o conflict\n",
                s, req_ts, optype == OPTYPE_LOOKUP ? "lookup":"update", e->key);

        if (optype == OPTYPE_LOOKUP) {
            e->ref_count++;
            r = 1;
        } else {
            e->ref_count = DATA_READY_MASK | 2;
            r = 1;
        }

        // insert a lock in the owners list
//        target = plmalloc_alloc(p, sizeof(struct lock_entry));
//        assert(target);
//        target->ts = req_ts;
//        target->s = s;
//        target->task_id = g_tid;
//        target->optype = optype;
        target->ready = 1;

        TAILQ_INSERT_HEAD(&e->owners, target, next);

        if (!TAILQ_EMPTY(&e->waiters)) {
            TAILQ_FOREACH(l, &e->waiters, next) {
            	int trg_srv = l->task_id;
                dreadlock_add(g_tid, req_ts, &trg_srv, &l->ts, 1);
            }
        }

    } else {
        dprint("srv(%d-%"PRIu64"): There is conflict!!\n", s, req_ts);


        TAILQ_FOREACH(l, &e->owners, next) {
        	dep_srv[all_deps] = l->task_id;
        	dep_tid[all_deps] = l->ts;
			all_deps++;
		}
		TAILQ_FOREACH(l, &e->waiters, next) {
			dep_srv[all_deps] = l->task_id;
			dep_tid[all_deps] = l->ts;
			all_deps++;
		}
		wait = 1;


//        int owners_cnt = 0;
//        int waiters_cnt = 0;
//
//        TAILQ_FOREACH(l, &e->owners, next) {
//            owners_cnt ++;
//        }
//        TAILQ_FOREACH(l, &e->waiters, next) {
//            waiters_cnt ++;
//        }
//        txncnt = owners_cnt + waiters_cnt;
//        wait = 1;
//        uint64_t txnid = g_tid;
//        if (txncnt) {
//        	int trg_srv[txncnt];
//			uint64_t trg_tid[txncnt];
//            int cur = 0;
//            TAILQ_FOREACH(l, &e->owners, next) {
//            	trg_srv[cur] = l->task_id;
//            	trg_tid[cur] = l->ts;
//                cur++;
//            }
//            TAILQ_FOREACH(l, &e->waiters, next) {
//            	trg_srv[cur] = l->task_id;
//				trg_tid[cur] = l->ts;
//                cur++;
//            }
//            dreadlock_add(g_tid, req_ts, trg_srv, trg_tid, cur);
//        }

#if DEBUG
		dprint("srv(%d-%"PRIu64"): %s lock request for key %"PRIu64" put under wait\n",
				s, req_ts, optype == OPTYPE_LOOKUP ? "lookup":"update", e->key);

		// if we are allowed to wait, make a new lock entry and add it to
		// waiters list
		TAILQ_FOREACH(l, &e->owners, next) {
			dprint("srv(%d-%"PRIu64"): Waiting for owner %d\n", s, req_ts, l->task_id);
		}
		TAILQ_FOREACH(l, &e->waiters, next) {
			dprint("srv(%d-%"PRIu64"): Waiting for waiter %d\n", s, req_ts, l->task_id);
		}
#endif //DEBUG
//		target = plmalloc_alloc(p, sizeof(struct lock_entry));
//		assert(target);
//		target->ts = req_ts;
//		target->s = s;
//		target->task_id = g_tid;
//		target->optype = optype;
		target->ready = 0;
		TAILQ_INSERT_HEAD(&e->waiters, target, next);

		dprint("srv(%d-%"PRIu64"): DONE Preparing waiter list\n", s, req_ts);
    }

    LATCH_RELEASE(&e->latch, &alock_state);
    dprint("srv(%d-%"PRIu64"): Released the latch and going to wait\n", s, req_ts);
    if (all_deps) {
		dreadlock_add(g_tid, req_ts, dep_srv, dep_tid, all_deps);
	}
    if (wait) {
        assert(r == 0);

        /* now spin until another thread signals us that we have the lock */
        assert(target);

        dprint("srv(%d-%"PRIu64"): %s lock request for key %"PRIu64" spinning until ready\n",
                s, req_ts, optype == OPTYPE_LOOKUP ? "lookup":"update", e->key);

        int deadlock = 0;
        int found_waiter = 0;
#if MIGRATION
        while (!target->ready) {
            task_yield(p, TASK_STATE_READY);
        }
#else
        // check for deadlocks
        deadlock = dreadlock_wait(g_tid, req_ts, &target->ready);
        //TODO: If cycle detection is removed, the next line must be un-commented
//        while (!target->ready);
        if (deadlock) {
#if ENABLE_KEY_SORTING
      assert(0);
#endif
        	// acquire the latch and remove from the waiters
        	dprint("srv(%d-%"PRIu64"): Acquiring the latch for key %"PRIu64"\n", s, req_ts, e->key);
        	LATCH_ACQUIRE(&e->latch, &alock_state);
        	dprint("srv(%d-%"PRIu64"): Acquired the latch for key %"PRIu64"\n", s, req_ts, e->key);

        	TAILQ_FOREACH(l, &e->waiters, next) {
        		if ((l->s == s) && (l->ts == req_ts)) {
        			TAILQ_REMOVE(&e->waiters, l, next);
        			found_waiter = 1;
        			break;
        		}
        	}

        	LATCH_RELEASE(&e->latch, &alock_state);
        	dprint("srv(%d-%"PRIu64"): Released the latch for key %"PRIu64"\n", s, req_ts, e->key);
        }
#endif
        if ((!deadlock) || (!found_waiter)) {
        	dprint("srv(%d-%"PRIu64"): %s lock request for key %"PRIu64" stopped spinning\n",
					s, req_ts, optype == OPTYPE_LOOKUP ? "lookup":"update", e->key);

			if (optype == OPTYPE_LOOKUP) {
				assert(e->ref_count > 1);
			} else {
				assert((e->ref_count & DATA_READY_MASK) &&
						((e->ref_count & ~DATA_READY_MASK) == 2));
			}

			r = 1;
        }

    }
#else
    assert(g_benchmark == &micro_bench && g_write_threshold == 100);
    r = 1;

#endif //SE_LATCH

    return r;
}


void selock_dl_detect_release(struct partition *p, struct op_ctx *octx)
{
    int alock_state;
    int s = p - &hash_table->partitions[0];
    int g_tid = p->current_task->g_tid;
    struct elem *e = octx->e;

    //	printf("Releasing the lock\n");
    dprint("srv(%d): Releasing lock on key %d\n", s, e->key);

#if SE_LATCH
    LATCH_ACQUIRE(&e->latch, &alock_state);

    dprint("srv(%d): releasing lock request for key %"PRIu64"\n", s, e->key);

    /* go through the owners list and remove the txn */
    struct lock_entry *lock_entry;

    int found_in_owners = 0;
    TAILQ_FOREACH(lock_entry, &e->owners, next) {
        if (lock_entry->task_id == g_tid) {
            found_in_owners = 1;
            break;
        }
    }

    if (!found_in_owners) {
        TAILQ_FOREACH(lock_entry, &e->owners, next) {
            if (lock_entry->task_id == g_tid) {
                break;
            }
        }
    }

    if (!lock_entry) {
        printf("srv(%d): FAILED releasing lock request for key %"PRIu64"\n", s, e->key);
    }

    assert(lock_entry);

    TAILQ_REMOVE(&e->owners, lock_entry, next);

    // decrement ref count as a owner is releasing a lock
    e->ref_count = (e->ref_count & ~DATA_READY_MASK) - 1;

    plmalloc_free(p, lock_entry, sizeof(*lock_entry));

    // if there are no more owners, then refcount should be 1
    if (TAILQ_EMPTY(&e->owners))
        assert(e->ref_count == 1);

    /* If lock_free is set, that means the new lock mode is decided by
     * the head of waiter list. If lock_free is not set, we still have
     * some readers. So only pending readers can be allowed. Keep
     * popping items from wait list as long as we have readers.
     */
    lock_entry = TAILQ_LAST(&e->waiters, lock_tail);
    while (lock_entry) {
        char conflict = 0;

        dprint("srv(%d): release request for key %"PRIu64" found %d waiting\n",
                s, e->key, lock_entry->task_id);

        if (lock_entry->optype == OPTYPE_LOOKUP) {
            conflict = !is_value_ready(e);
        } else {
            assert(lock_entry->optype == OPTYPE_UPDATE);
            conflict = (!is_value_ready(e)) || ((e->ref_count & ~DATA_READY_MASK) > 1);
        }

        if (conflict) {
            dprint("srv(%d): release request for key %"PRIu64" found %d in conflict "
                    "ref count was %"PRIu64"\n", s, e->key, lock_entry->task_id, e->ref_count);
            break;

        } else {
            /* there's no conflict only if there is a shared lock and we're
             * requesting a shared lock, or if there's no lock
             */
            assert((e->ref_count & DATA_READY_MASK) == 0);

            if (lock_entry->optype == OPTYPE_LOOKUP) {
                assert((e->ref_count & ~DATA_READY_MASK) >= 1);
                e->ref_count++;
            } else {
                assert((e->ref_count & ~DATA_READY_MASK) == 1);
                e->ref_count = DATA_READY_MASK | 2;
            }
        }

        // remove from waiters, add to owners, mark as ready
        TAILQ_REMOVE(&e->waiters, lock_entry, next);
        TAILQ_INSERT_HEAD(&e->owners, lock_entry, next);
        lock_entry->ready = 1;

        dprint("srv(%d): release lock request for key %"PRIu64" marking %d as ready\n",
                s, e->key, lock_entry->task_id);

        // go to next element
        lock_entry = TAILQ_LAST(&e->waiters, lock_tail);
    }

    LATCH_RELEASE(&e->latch, &alock_state);

#else
    assert(g_benchmark == &micro_bench && g_write_threshold == 100);
#endif // SE_LATCH

    /* WAIT DIE */


    dprint("srv(%d): Released lock on key %d\n", s, e->key);
}
#endif // ENABLE_CYCLE_DETECTION
#endif

int selock_acquire(struct partition *p, struct elem *e, 
    char optype, uint64_t req_ts)
{
#if ENABLE_WAIT_DIE_CC
    return selock_wait_die_acquire(p, e, optype, req_ts);
#elif ENABLE_NOWAIT_OWNER_CC
    return selock_nowait_with_ownerlist_acquire(p, e, optype, req_ts);
#elif ENABLE_NOWAIT_CC
    return selock_nowait_acquire(p, e, optype, req_ts);
#elif ENABLE_SILO_CC
    // with silo, nothing to do
    return 1;
#elif ENABLE_DL_DETECT_CC
    return selock_dl_detect_acquire(p, e, optype, req_ts);
#endif
}

void selock_release(struct partition *p, struct op_ctx *octx)
{
#if ENABLE_WAIT_DIE_CC
  return selock_wait_die_release(p, octx);
#elif ENABLE_NOWAIT_OWNER_CC
  return selock_nowait_with_ownerlist_release(p, octx);
#elif ENABLE_NOWAIT_CC
  return selock_nowait_release(p, octx);
#elif defined(ENABLE_SILO_CC) || defined(ENABLE_MV2PL)
  // with silo, nothing to do
  return;
#elif ENABLE_DL_DETECT_CC
  return selock_dl_detect_release(p, octx);
#endif
}

//#endif
