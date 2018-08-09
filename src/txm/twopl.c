#include "headers.h"
#include "smphashtable.h"
#include "plmalloc.h"

#if ENABLE_DL_DETECT_CC
#include "se_dl_detect_graph.h"
#include "glo.h"
#endif

extern int write_threshold;

#if defined(ENABLE_WAIT_DIE_CC) || defined(ENABLE_BWAIT_CC) || defined(ENABLE_SILO_CC)
int wait_die_check_acquire(int s, struct partition *p,
    int c, int tid, int opid, struct elem *e, char optype, uint64_t req_ts)
{
  char conflict;
  int r;
  struct lock_entry *l;

  if (optype == OPTYPE_LOOKUP) {
    conflict = !is_value_ready(e);
  } else {
    assert(optype == OPTYPE_UPDATE);
    conflict = !is_value_ready(e) || e->ref_count > 1;
  }

  if (!conflict) {
    if (!LIST_EMPTY(&e->waiters)) {
      l = LIST_FIRST(&e->waiters);
      if (req_ts <= l->ts)
        conflict = 1;
    }
  }

  if (conflict) {
    /* There was a conflict. In wait die case, we can wait if
     * req_ts is < ts of all owner txns
     */
    int wait = 1;
    LIST_FOREACH(l, &e->owners, next) {
      if (l->ts < req_ts || ((l->ts == req_ts) && (l->s < c)) ||
          ((l->ts == req_ts) && (l->s == c) && (l->task_id < tid))) {
        wait = 0;
        break;
      }
    }

    if (wait) {
      r = LOCK_WAIT;
    } else {
      r = LOCK_ABORT;
    }
  } else {
    r = LOCK_SUCCESS;
  }

  return r;
}

int wait_die_acquire(int s, struct partition *p,
    int c, int task_id, int op_id, struct elem *e, char optype,
    uint64_t req_ts, struct lock_entry **pl)
{
  struct lock_entry *l;
  int r, conflict;

  *pl = NULL;

#if VERIFY_CONSISTENCY
  /* we cannot be on the owners list */
  LIST_FOREACH(l, &e->owners, next) {
    if (l->s == c && l->task_id == task_id && l->op_id == op_id)
      assert(0);
  }

  /* we cannot be on the waiters list */
  LIST_FOREACH(l, &e->waiters, next) {
    if (l->s == c && l->task_id == task_id && l->op_id == op_id)
      assert(0);
  }
#endif

  dprint("srv(%d): cl %d %" PRIu64 " rc %" PRIu64 "\n", s,
      c, e->key, e->ref_count);

  if (optype == OPTYPE_LOOKUP) {
    conflict = !is_value_ready(e);
  } else {
    assert(optype == OPTYPE_UPDATE || optype == OPTYPE_INSERT);
    conflict = (!is_value_ready(e)) || (e->ref_count > 1);
  }

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

  /* if somebody has lock or wait die is signaling a conflict
   * we need to check if this txn must wait or abort. If it must
   * wait, then we should add it to waiters list and continue.
   * Sometime later, someone will move us to the owner list and reply back
   * to the requestor
   *
   * If it must abort, then we should unlock all locks acquired,
   * then return back a NULL for each message.

  */
  if (conflict) {

    /* There was a conflict. In wait die case, we can wait if
     * req_ts is < ts of all owner txns
     */
    int wait = 1;
    LIST_FOREACH(l, &e->owners, next) {
      if (l->ts < req_ts || ((l->ts == req_ts) && (l->s < c)) ||
          ((l->ts == req_ts) && (l->s == c) && (l->task_id < task_id))) {
        wait = 0;
        break;
      }
    }

    if (wait) {

      dprint("srv(%d): cl %d %"PRIu64" rc %"PRIu64" waiting \n",
          s, c, e->key, e->ref_count);

      // if we are allowed to wait, make a new lock entry and add it to
      // waiters list.
      struct lock_entry *last_lock = NULL;
      LIST_FOREACH(l, &e->waiters, next) {

        /* there cannot be a request already from same server/task/op combo */
        if (l->s == c && l->task_id == task_id && l->op_id == op_id)
          assert(0);

        // break if this is where we have to insert ourself
        if (l->ts < req_ts || (l->ts == req_ts && l->s < c) ||
          ((l->ts == req_ts) && (l->s == c) && (l->task_id < task_id)))
          break;

        last_lock = l;
      }

      struct lock_entry *target = plmalloc_alloc(p, sizeof(struct lock_entry));
      assert(target);
      target->ts = req_ts;
      target->s = c;

      target->optype = optype;
      target->ready = 0;
      target->task_id = task_id;
      target->op_id = op_id;
      *pl = target;

      if (l) {
        LIST_INSERT_BEFORE(l, target, next);
      } else {
        if (last_lock) {
          LIST_INSERT_AFTER(last_lock, target, next);
        } else {
          LIST_INSERT_HEAD(&e->waiters, target, next);
        }
      }

      r = LOCK_WAIT;
    } else {
      dprint("srv(%d): cl %d %"PRIu64" rc %"PRIu64" aborted \n",
          s, c, e->key, e->ref_count);

      r = LOCK_ABORT;
    }
  } else {

    if (optype == OPTYPE_LOOKUP)
      e->ref_count++;
    else
      e->ref_count = DATA_READY_MASK | 2;

    // insert a lock in the owners list
    struct lock_entry *target = plmalloc_alloc(p, sizeof(struct lock_entry));
    assert(target);
    target->ts = req_ts;
    target->s = c;
    target->optype = optype;
    target->ready = 1;
    target->task_id = task_id;
    target->op_id = op_id;
    *pl = target;

    dprint("srv(%d): cl %d %" PRIu64 " rc %" PRIu64
        " adding to owners\n", s, c, e->key, e->ref_count);

    LIST_INSERT_HEAD(&e->owners, target, next);

    r = LOCK_SUCCESS;
  }

  return r;
}

void wait_die_release(int s, struct partition *p, int c, int task_id,
    int op_id, struct elem *e)
{
  // find out lock on the owners list
  struct lock_entry *l;
  LIST_FOREACH(l, &e->owners, next) {
    if (l->s == c && l->task_id == task_id && l->op_id == op_id)
      break;
  }

  if (l) {
    // free lock
    dprint("srv(%d): cl %d key %" PRIu64 " rc %" PRIu64
        " being removed from owners\n", s, c, e->key, e->ref_count);

    LIST_REMOVE(l, next);

    mp_release_value_(p, e);
  } else {
    // it is possible that lock is on waiters list. Imagine a scenario
    // where we send requests in bulk to 2 servers, one waits and other
    // aborts. In this case, we will get release message for a request
    // that is currently waiting
    LIST_FOREACH(l, &e->waiters, next) {
      if (l->s == c && l->task_id == task_id && l->op_id == op_id)
        break;
    }

    // can't be on neither owners nor waiters!
    assert(l);
    LIST_REMOVE(l, next);
  }

  plmalloc_free(p, l, sizeof(struct lock_entry));

  // if there are no more owners, then refcount should be 1
  if (!LIST_EMPTY(&e->owners)) {
    assert(e->ref_count != 1);
    return;
  } else {
    assert(e->ref_count == 1);
  }

  /* Go through wait list and find new owner(s) */
  l = LIST_FIRST(&e->waiters);
  while (l) {
    char conflict = 0;

    if (l->optype == OPTYPE_LOOKUP) {
      conflict = !is_value_ready(e);
    } else {
      assert(l->optype == OPTYPE_UPDATE);
      conflict = (!is_value_ready(e)) || ((e->ref_count & ~DATA_READY_MASK) > 1);
    }

    if (conflict) {
      break;
    } else {
      /* there's no conflict only if there is a shared lock and we're
       * requesting a shared lock, or if there's no lock
       */
      assert((e->ref_count & DATA_READY_MASK) == 0);

      if (l->optype == OPTYPE_LOOKUP) {
        assert((e->ref_count & ~DATA_READY_MASK) >= 1);
        e->ref_count++;
      } else {
        assert((e->ref_count & ~DATA_READY_MASK) == 1);
        e->ref_count = DATA_READY_MASK | 2;
      }
    }

    // remove from waiters, add to owners, mark as ready
    LIST_REMOVE(l, next);
    LIST_INSERT_HEAD(&e->owners, l, next);

    // mark as ready and send message to server
    l->ready = 1;
    mp_send_reply(s, l->s, l->task_id, l->op_id, e);

    dprint("srv(%d): release lock request for key %"PRIu64" marking %d as ready\n",
        s, e->key, l->s);

    // go to next element
    l = LIST_FIRST(&e->waiters);
  }
}
#endif//IF_WAIT_DIE

#if ENABLE_NOWAIT_CC
void no_wait_release(struct partition *p, struct elem *e)
{
  mp_release_value_(p, e);
}

int no_wait_check_acquire(struct elem *e, char optype)
{
  int r;

  if (optype == OPTYPE_LOOKUP) {
    if (!is_value_ready(e)) {
      r = LOCK_ABORT;
    } else {
      r = LOCK_SUCCESS;
    }
  } else {
    assert(optype == OPTYPE_UPDATE || optype == OPTYPE_INSERT);
    if (!is_value_ready(e) || e->ref_count > 1) {
      r = LOCK_ABORT;
    } else {
      r = LOCK_SUCCESS;
    }
  }

  return r;
}

int no_wait_acquire(struct elem *e, char optype)
{
  int r = no_wait_check_acquire(e, optype);

  if (r == LOCK_ABORT)
    return r;

  if (optype == OPTYPE_LOOKUP) {
    e->ref_count++;
  } else {
    assert(optype == OPTYPE_UPDATE || optype == OPTYPE_INSERT);
    e->ref_count = DATA_READY_MASK | 2;
  }

  return LOCK_SUCCESS;
}


#endif//IF_NOWAIT

#if defined(ENABLE_BWAIT_CC) || defined(ENABLE_SILO_CC)
int bwait_acquire(int s, struct partition *p,
    int c, int task_id, int op_id, struct elem *e, char optype,
    struct lock_entry **pl)
{
  struct lock_entry *l;
  int r, conflict;

  *pl = NULL;

#if VERIFY_CONSISTENCY
  /* we cannot be on the owners list */
  LIST_FOREACH(l, &e->owners, next) {
    if (l->s == c && l->task_id == task_id && l->op_id == op_id)
      assert(0);
  }

  /* we cannot be on the waiters list */
  LIST_FOREACH(l, &e->waiters, next) {
    if (l->s == c && l->task_id == task_id && l->op_id == op_id)
      assert(0);
  }
#endif

  dprint("srv(%d): cl %d %" PRIu64 " rc %" PRIu64 "\n", s,
      c, e->key, e->ref_count);

  if (optype == OPTYPE_LOOKUP) {
    conflict = !is_value_ready(e);
  } else {
    assert(optype == OPTYPE_UPDATE || optype == OPTYPE_INSERT);
    conflict = (!is_value_ready(e)) || (e->ref_count > 1);
  }

  // if there is a conflict, add to lock list
  if (conflict) {
    struct lock_entry *target = plmalloc_alloc(p, sizeof(struct lock_entry));
    assert(target);
    target->s = c;

    target->optype = optype;
    target->ready = 0;
    target->task_id = task_id;
    target->op_id = op_id;
    *pl = target;

    LIST_INSERT_HEAD(&e->waiters, target, next);
    r = LOCK_WAIT;
  } else {

    if (optype == OPTYPE_LOOKUP)
      e->ref_count++;
    else
      e->ref_count = DATA_READY_MASK | 2;

    // insert a lock in the owners list
    struct lock_entry *target = plmalloc_alloc(p, sizeof(struct lock_entry));
    assert(target);
    target->s = c;
    target->optype = optype;
    target->ready = 1;
    target->task_id = task_id;
    target->op_id = op_id;
    *pl = target;

    dprint("srv(%d): cl %d %" PRIu64 " rc %" PRIu64
        " adding to owners\n", s, c, e->key, e->ref_count);

    LIST_INSERT_HEAD(&e->owners, target, next);

    r = LOCK_SUCCESS;
  }

  return r;
}

void bwait_release(int s, struct partition *p, int c, int task_id,
    int op_id, struct elem *e)
{
    return wait_die_release(s, p, c, task_id, op_id, e);
}

int bwait_check_acquire(struct elem *e, char optype)
{
  int r;

  if (optype == OPTYPE_LOOKUP) {
    if (!is_value_ready(e)) {
      r = LOCK_WAIT;
    } else {
      r = LOCK_SUCCESS;
    }
  } else {
    assert(optype == OPTYPE_UPDATE || optype == OPTYPE_INSERT);
    if (!is_value_ready(e) || e->ref_count > 1) {
      r = LOCK_WAIT;
    } else {
      r = LOCK_SUCCESS;
    }
  }

  return r;

}
#endif//IF_BWAIT


#if ENABLE_DL_DETECT_CC
struct se_dl_detect_graph_node *dep_graph_nodes;

void dl_detect_init_data_structures() {
	int all_servers = g_nservers * g_nfibers;
	dep_graph_nodes = (struct se_dl_detect_graph_node *) malloc(all_servers * sizeof(struct se_dl_detect_graph_node));
	for (int i = 0; i < all_servers; i ++) {
		dep_graph_nodes[i].neighbors = (struct se_waiter_node *) malloc(all_servers * sizeof(struct se_waiter_node));
	}
}

int not_two_readers_wo(struct lock_entry *lt, struct lock_entry *le) {
	if ((lt->optype == OPTYPE_LOOKUP) && (le->optype == OPTYPE_LOOKUP)) {
		return 0;
	}
	return 1;
}

int not_two_readers_ww(struct lock_entry *lt, struct lock_entry *le) {
	if ((lt->optype == OPTYPE_LOOKUP) && (le->optype == OPTYPE_LOOKUP)) {
		return 0;
	}
	return 1;
}

int add_dependencies(int s, struct partition *p,
		struct lock_entry *inserted_waiter, struct elem *e) {
	uint64_t node_id = inserted_waiter->s * g_nfibers + inserted_waiter->task_id - 2;
	dep_graph_nodes[node_id].e = e;
	dep_graph_nodes[node_id].opid = inserted_waiter->op_id;
	dep_graph_nodes[node_id].sender_srv = s;
	dep_graph_nodes[node_id].srvfib = node_id;
	dep_graph_nodes[node_id].ts = inserted_waiter->ts;
	dep_graph_nodes[node_id].waiters_size = 0;

	struct lock_entry *nxt = TAILQ_NEXT(inserted_waiter, next);
	// if a reader was inserted
	if (inserted_waiter->optype == OPTYPE_LOOKUP) {
		// find the next writer
		while ((nxt != NULL) && (nxt->optype == OPTYPE_LOOKUP)){

			nxt = TAILQ_NEXT(nxt, next);
		}
		if (nxt != NULL) {
			assert(not_two_readers_ww(inserted_waiter, nxt));
			// this is a writer, so add a dependency
			dep_graph_nodes[node_id].neighbors[dep_graph_nodes[node_id].waiters_size].opid = nxt->op_id;
			dep_graph_nodes[node_id].neighbors[dep_graph_nodes[node_id].waiters_size].srvfib = nxt->s * g_nfibers + nxt->task_id - 2;
			dep_graph_nodes[node_id].neighbors[dep_graph_nodes[node_id].waiters_size].ts = nxt->ts;
			dep_graph_nodes[node_id].waiters_size++;
		} else {
			// add a dependency to the owner

			struct lock_entry *owner;
			int readers = 0;
			TAILQ_FOREACH(owner, &e->owners, next) {
        
				if (owner->optype != OPTYPE_UPDATE) {
					
					assert(0);
					break;
				}
				assert(not_two_readers_wo(inserted_waiter, owner));
				dep_graph_nodes[node_id].neighbors[dep_graph_nodes[node_id].waiters_size].opid = owner->op_id;
				dep_graph_nodes[node_id].neighbors[dep_graph_nodes[node_id].waiters_size].srvfib = owner->s * g_nfibers + owner->task_id - 2;
				dep_graph_nodes[node_id].neighbors[dep_graph_nodes[node_id].waiters_size].ts = owner->ts;
				dep_graph_nodes[node_id].waiters_size++;
			}

		}
	} else if (inserted_waiter->optype == OPTYPE_UPDATE || inserted_waiter->optype == OPTYPE_INSERT) {
		// if a writer was inserted and there is a next node
		if (nxt != NULL) {
			// add a dependency to the next waiter
			assert(not_two_readers_ww(inserted_waiter, nxt));
			dep_graph_nodes[node_id].neighbors[dep_graph_nodes[node_id].waiters_size].opid = nxt->op_id;
			dep_graph_nodes[node_id].neighbors[dep_graph_nodes[node_id].waiters_size].srvfib = nxt->s * g_nfibers + nxt->task_id - 2;
			dep_graph_nodes[node_id].neighbors[dep_graph_nodes[node_id].waiters_size].ts = nxt->ts;
			dep_graph_nodes[node_id].waiters_size++;
			// only if added a lookup node, move forward
			if (nxt->optype == OPTYPE_LOOKUP) {
				// move forward
				nxt = TAILQ_NEXT(nxt, next);
			}
		} else {
			// add dependency to the owner(s)
			struct lock_entry *owner;
			TAILQ_FOREACH(owner, &e->owners, next) {
				assert(not_two_readers_wo(inserted_waiter, owner));
				dep_graph_nodes[node_id].neighbors[dep_graph_nodes[node_id].waiters_size].opid = owner->op_id;
				dep_graph_nodes[node_id].neighbors[dep_graph_nodes[node_id].waiters_size].srvfib = owner->s * g_nfibers + owner->task_id - 2;
				dep_graph_nodes[node_id].neighbors[dep_graph_nodes[node_id].waiters_size].ts = owner->ts;
				dep_graph_nodes[node_id].waiters_size++;
			}
		}

		while ((nxt != NULL) && (nxt->optype == OPTYPE_LOOKUP)) {

			assert(not_two_readers_ww(inserted_waiter, nxt));
			// add a dependency
			dep_graph_nodes[node_id].neighbors[dep_graph_nodes[node_id].waiters_size].opid = nxt->op_id;
			dep_graph_nodes[node_id].neighbors[dep_graph_nodes[node_id].waiters_size].srvfib = nxt->s * g_nfibers + nxt->task_id - 2;
			dep_graph_nodes[node_id].neighbors[dep_graph_nodes[node_id].waiters_size].ts = nxt->ts;
			dep_graph_nodes[node_id].waiters_size++;
			// move forward
			nxt = TAILQ_NEXT(nxt, next);
		}
	} else {
		assert(0);
	}

	int deadlock = 0;
	if (se_dl_detect_add_dependency(&dep_graph_nodes[node_id])) {
		deadlock = se_dl_detect_detect_cycle(s, &dep_graph_nodes[node_id]);
		if (deadlock) {
			se_dl_detect_clear_dependencies(&dep_graph_nodes[node_id], 1);
		}
	}
	return deadlock;
}

void update_dependencies(int s, struct partition *p, struct lock_entry *to_remove, struct elem *e) {
	struct dep_entry {
		uint64_t msg[2];
		LIST_ENTRY(dep_entry) deps;
	};
	LIST_HEAD(dep_list, dep_entry);
	struct dep_list dependencies;
	LIST_INIT(&dependencies);

	struct dep_list clr_dependencies;
	LIST_INIT(&clr_dependencies);

	int added_dependencies = 0;
	int cleared_dependencies = 0;

	struct box_array *boxes = hash_table->boxes;
	// if we remove a reader
	if (to_remove->optype == OPTYPE_LOOKUP) {
		// find the first writer before the element to remove
		struct lock_entry *lq = TAILQ_PREV(to_remove, lock_tail, next);
		while ((lq != NULL) && (lq->optype == OPTYPE_LOOKUP)) {
			lq = TAILQ_PREV(lq, lock_tail, next);
		}
		// if there is a writer somewhere before the reader to be removed
		if (lq != NULL) {
			// break the dependency between that writer and the element to remove
			uint64_t clr_msg[4];
			clr_msg[0] = MAKE_HASH_MSG(lq->task_id - 2, lq->s, (unsigned long) s, DL_DETECT_RMV_DEP_SRC);
			clr_msg[1] = MAKE_TS_MSG(lq->op_id, lq->ts);
			struct dep_entry *src_clr_entry = plmalloc_alloc(p, sizeof(struct dep_entry));
			memcpy(src_clr_entry->msg, &clr_msg[0], 2 * sizeof(uint64_t));

			dprint("srv(%d)-DL-DETECT_Release: Removing dependencies for src (%d,%d,%ld)\n", s, lq->s, lq->task_id - 2, lq->ts);

			clr_msg[2] = MAKE_HASH_MSG(to_remove->task_id - 2, to_remove->s, (unsigned long) s, DL_DETECT_RMV_DEP_TRG);
			clr_msg[3] = MAKE_TS_MSG(to_remove->op_id, to_remove->ts);
			struct dep_entry *dst_clr_entry = plmalloc_alloc(p, sizeof(struct dep_entry));
			memcpy(dst_clr_entry->msg, &clr_msg[2], 2 * sizeof(uint64_t));
			LIST_INSERT_HEAD(&clr_dependencies, dst_clr_entry, deps);
			LIST_INSERT_HEAD(&clr_dependencies, src_clr_entry, deps);
			cleared_dependencies += 4;
			dprint("srv(%d)-DL-DETECT_Release: Removing dependencies for trg (%d,%d,%ld)\n", s, to_remove->s, to_remove->task_id - 2, to_remove->ts);


			/* then add a dependency between lq and the next node of to_remove if there are no more readers
			 * if the previous of to_remove is a reader, then the previous writer will already have dependencies
			 * to all the readers of the group
			 */
			if (TAILQ_PREV(to_remove, lock_tail, next)->optype != OPTYPE_LOOKUP) {
				struct lock_entry *nxt = TAILQ_NEXT(to_remove, next);
				// if the element to remove is not last waiter
				if (nxt != NULL) {
					// if the next of to_remove is a reader, then dependencies are already there
					if (nxt->optype != OPTYPE_LOOKUP) {
						assert(not_two_readers_ww(lq, nxt));
						uint64_t add_msg[4];
						add_msg[0] = MAKE_HASH_MSG(lq->task_id - 2, lq->s, (unsigned long) e, DL_DETECT_ADD_DEP_SRC);
						add_msg[1] = MAKE_TS_MSG(lq->op_id, lq->ts);
						struct dep_entry *new_src_entry = plmalloc_alloc(p, sizeof(struct dep_entry));
						memcpy(new_src_entry->msg, &add_msg[0], 2 * sizeof(uint64_t));
						LIST_INSERT_HEAD(&dependencies, new_src_entry, deps);
						add_msg[2] = MAKE_HASH_MSG(nxt->task_id - 2, nxt->s, (unsigned long) e, DL_DETECT_ADD_DEP_TRG);
						add_msg[3] = MAKE_TS_MSG(nxt->op_id, nxt->ts);
						struct dep_entry *new_dst_entry = plmalloc_alloc(p, sizeof(struct dep_entry));
						memcpy(new_dst_entry->msg, &add_msg[2], 2 * sizeof(uint64_t));
						LIST_INSERT_AFTER(new_src_entry, new_dst_entry, deps);
						added_dependencies += 4;
					}
				} else {
					// there are no more waiters; add the owners
					struct lock_entry *l;
					int owners = 0;
					TAILQ_FOREACH(l, &e->owners, next) {
						owners ++;
					}
					uint64_t add_msg[2 * owners + 2];
					int msg_cnt = 0;
					add_msg[msg_cnt++] = MAKE_HASH_MSG(lq->task_id - 2, lq->s, (unsigned long) e, DL_DETECT_ADD_DEP_SRC);
					add_msg[msg_cnt++] = MAKE_TS_MSG(lq->op_id, lq->ts);
					struct dep_entry *new_src_entry = plmalloc_alloc(p, sizeof(struct dep_entry));
					memcpy(new_src_entry->msg, &add_msg[0], 2 * sizeof(uint64_t));
					LIST_INSERT_HEAD(&dependencies, new_src_entry, deps);
					added_dependencies += 2;
					TAILQ_FOREACH(l, &e->owners, next) {
						assert(not_two_readers_wo(lq, l));
						add_msg[msg_cnt++] = MAKE_HASH_MSG(l->task_id - 2, l->s, (unsigned long) e, DL_DETECT_ADD_DEP_TRG);
						add_msg[msg_cnt++] = MAKE_TS_MSG(l->op_id, l->ts);
						struct dep_entry *new_dst_entry = plmalloc_alloc(p, sizeof(struct dep_entry));
						memcpy(new_dst_entry->msg, &add_msg[msg_cnt - 2], 2 * sizeof(uint64_t));
						LIST_INSERT_AFTER(new_src_entry, new_dst_entry, deps);
						added_dependencies += 2;
					}
				}
			}
		}
	} else if (to_remove->optype == OPTYPE_INSERT || to_remove->optype == OPTYPE_UPDATE) {	// we remove a writer
		struct lock_entry *lq = TAILQ_PREV(to_remove, lock_tail, next);
		int have_readers = 0;
		int have_readers_after = 0;

		// go back to the tail until a writer is found or it finishes
		while (lq != NULL) {
			// break the dependency between lq and the element to remove, which is a writer
			uint64_t clr_msg[4];
			clr_msg[0] = MAKE_HASH_MSG(lq->task_id - 2, lq->s, (unsigned long) s, DL_DETECT_RMV_DEP_SRC);
			clr_msg[1] = MAKE_TS_MSG(lq->op_id, lq->ts);
			struct dep_entry *src_clr_entry = plmalloc_alloc(p, sizeof(struct dep_entry));
			memcpy(src_clr_entry->msg, &clr_msg[0], 2 * sizeof(uint64_t));
			dprint("srv(%d)-DL-DETECT_Release: Removing dependencies for src (%d,%d,%ld)\n", s, lq->s, lq->task_id - 2, lq->ts);

			clr_msg[2] = MAKE_HASH_MSG(to_remove->task_id - 2, to_remove->s, (unsigned long) s, DL_DETECT_RMV_DEP_TRG);
			clr_msg[3] = MAKE_TS_MSG(to_remove->op_id, to_remove->ts);
			struct dep_entry *dst_clr_entry = plmalloc_alloc(p, sizeof(struct dep_entry));
			memcpy(dst_clr_entry->msg, &clr_msg[2], 2 * sizeof(uint64_t));
			LIST_INSERT_HEAD(&clr_dependencies, dst_clr_entry, deps);
			LIST_INSERT_HEAD(&clr_dependencies, src_clr_entry, deps);
			cleared_dependencies += 4;

			dprint("srv(%d)-DL-DETECT_Release: Removing dependencies for trg (%d,%d,%ld)\n", s, to_remove->s, to_remove->task_id - 2, to_remove->ts);

			// add a dependency to the next element of the element to remove
			struct lock_entry *nxt = TAILQ_NEXT(to_remove, next);
			/* if the previous element is a reader, then we need to find the first writer or the owner(s)
			 * and link all previous readers as well
			 */
			if (lq->optype == OPTYPE_LOOKUP) {
				have_readers = 1;
				while ((nxt != NULL) && (nxt->optype == OPTYPE_LOOKUP)) {
					have_readers_after = 1;
					nxt = TAILQ_NEXT(nxt, next);
				}
				// there is a writer somewhere after the element to remove
				if (nxt != NULL) {
					assert(not_two_readers_ww(lq, nxt));
					uint64_t add_msg[4];
					add_msg[0] = MAKE_HASH_MSG(lq->task_id - 2, lq->s, (unsigned long) e, DL_DETECT_ADD_DEP_SRC);
					add_msg[1] = MAKE_TS_MSG(lq->op_id, lq->ts);
					struct dep_entry *new_src_entry = plmalloc_alloc(p, sizeof(struct dep_entry));
					memcpy(new_src_entry->msg, &add_msg[0], 2 * sizeof(uint64_t));
					LIST_INSERT_HEAD(&dependencies, new_src_entry, deps);

					add_msg[2] = MAKE_HASH_MSG(nxt->task_id - 2, nxt->s, (unsigned long) e, DL_DETECT_ADD_DEP_TRG);
					add_msg[3] = MAKE_TS_MSG(nxt->op_id, nxt->ts);
					struct dep_entry *new_dst_entry = plmalloc_alloc(p, sizeof(struct dep_entry));
					memcpy(new_dst_entry->msg, &add_msg[2], 2 * sizeof(uint64_t));
					LIST_INSERT_AFTER(new_src_entry, new_dst_entry, deps);

					added_dependencies += 4;
				} else {
					// there are no more waiters; add the owners
					struct lock_entry *l;
					int owners = 0;
					TAILQ_FOREACH(l, &e->owners, next) {
						if (l->optype == OPTYPE_LOOKUP) {
							break;
						}
						owners ++;
					}
					if (owners) {
						uint64_t add_msg[2 * owners + 2];
						int msg_cnt = 0;
						add_msg[msg_cnt++] = MAKE_HASH_MSG(lq->task_id - 2, lq->s, (unsigned long) e, DL_DETECT_ADD_DEP_SRC);
						add_msg[msg_cnt++] = MAKE_TS_MSG(lq->op_id, lq->ts);
						struct dep_entry *new_src_entry = plmalloc_alloc(p, sizeof(struct dep_entry));
						memcpy(new_src_entry->msg, &add_msg[0], 2 * sizeof(uint64_t));
						LIST_INSERT_HEAD(&dependencies, new_src_entry, deps);
						added_dependencies += 2;
						TAILQ_FOREACH(l, &e->owners, next) {
							assert(not_two_readers_wo(lq, l));
							add_msg[msg_cnt++] = MAKE_HASH_MSG(l->task_id - 2, l->s, (unsigned long) e, DL_DETECT_ADD_DEP_TRG);
							add_msg[msg_cnt++] = MAKE_TS_MSG(l->op_id, l->ts);
							struct dep_entry *new_dst_entry = plmalloc_alloc(p, sizeof(struct dep_entry));
							memcpy(new_dst_entry->msg, &add_msg[msg_cnt - 2], 2 * sizeof(uint64_t));
							LIST_INSERT_AFTER(new_src_entry, new_dst_entry, deps);
							added_dependencies += 2;
						}
					}

				}
				// if the previous element is a reader, then we need to do the same for the previous element
				lq = TAILQ_PREV(lq, lock_tail, next);
				// otherwise, if the previous of the current reader is a writer or we have finished, we need to break out the loop
				if ((lq != NULL) && (lq->optype != OPTYPE_LOOKUP)) {
					break;
				}
			} else {	// the previous element is a writer
				// if we have readers as next elements, then we need a dependency for each one of these readers
				int readers = 0;
				while ((nxt != NULL) && (nxt->optype == OPTYPE_LOOKUP)) {
					readers++;
					nxt = TAILQ_NEXT(nxt, next);
				}
				if (readers) {
					nxt = TAILQ_NEXT(to_remove, next);
					uint64_t add_msg[2 * readers + 2];
					int msg_cnt = 0;
					add_msg[msg_cnt++] = MAKE_HASH_MSG(lq->task_id - 2, lq->s, (unsigned long) e, DL_DETECT_ADD_DEP_SRC);
					add_msg[msg_cnt++] = MAKE_TS_MSG(lq->op_id, lq->ts);
					struct dep_entry *new_src_entry = plmalloc_alloc(p, sizeof(struct dep_entry));
					memcpy(new_src_entry->msg, &add_msg[0], 2 * sizeof(uint64_t));
					LIST_INSERT_HEAD(&dependencies, new_src_entry, deps);
					added_dependencies += 2;
					while ((nxt != NULL) && (nxt->optype == OPTYPE_LOOKUP)) {
						assert(not_two_readers_ww(lq, nxt));
						add_msg[msg_cnt++] = MAKE_HASH_MSG(nxt->task_id - 2, nxt->s, (unsigned long) e, DL_DETECT_ADD_DEP_TRG);
						add_msg[msg_cnt++] = MAKE_TS_MSG(nxt->op_id, nxt->ts);
						struct dep_entry *new_dst_entry = plmalloc_alloc(p, sizeof(struct dep_entry));
						memcpy(new_dst_entry->msg, &add_msg[msg_cnt - 2], 2 * sizeof(uint64_t));
						LIST_INSERT_AFTER(new_src_entry, new_dst_entry, deps);
						added_dependencies += 2;
						nxt = TAILQ_NEXT(nxt, next);
					}
				} else {	// either we have a writer or the waiting list is done
					if (nxt != NULL) {	// we have a writer
						assert(not_two_readers_ww(lq, nxt));
						uint64_t add_msg[4];
						add_msg[0] = MAKE_HASH_MSG(lq->task_id - 2, lq->s, (unsigned long) e, DL_DETECT_ADD_DEP_SRC);
						add_msg[1] = MAKE_TS_MSG(lq->op_id, lq->ts);
						struct dep_entry *new_src_entry = plmalloc_alloc(p, sizeof(struct dep_entry));
						memcpy(new_src_entry->msg, &add_msg[0], 2 * sizeof(uint64_t));
						LIST_INSERT_HEAD(&dependencies, new_src_entry, deps);
						add_msg[2] = MAKE_HASH_MSG(nxt->task_id - 2, nxt->s, (unsigned long) e, DL_DETECT_ADD_DEP_TRG);
						add_msg[3] = MAKE_TS_MSG(nxt->op_id, nxt->ts);
						struct dep_entry *new_dst_entry = plmalloc_alloc(p, sizeof(struct dep_entry));
						memcpy(new_dst_entry->msg, &add_msg[2], 2 * sizeof(uint64_t));
						LIST_INSERT_AFTER(new_src_entry, new_dst_entry, deps);
						added_dependencies += 4;
					} else {	// there are no more waiters; add the owners
						struct lock_entry *l;
						int owners = 0;
						TAILQ_FOREACH(l, &e->owners, next) {
							owners ++;
						}
						uint64_t add_msg[2 * owners + 2];
						int msg_cnt = 0;
						add_msg[msg_cnt++] = MAKE_HASH_MSG(lq->task_id - 2, lq->s, (unsigned long) e, DL_DETECT_ADD_DEP_SRC);
						add_msg[msg_cnt++] = MAKE_TS_MSG(lq->op_id, lq->ts);
						struct dep_entry *new_src_entry = plmalloc_alloc(p, sizeof(struct dep_entry));
						memcpy(new_src_entry->msg, &add_msg[0], 2 * sizeof(uint64_t));
						LIST_INSERT_HEAD(&dependencies, new_src_entry, deps);
						added_dependencies += 2;
						TAILQ_FOREACH(l, &e->owners, next) {
							assert(not_two_readers_wo(lq, l));
							add_msg[msg_cnt++] = MAKE_HASH_MSG(l->task_id - 2, l->s, (unsigned long) e, DL_DETECT_ADD_DEP_TRG);
							add_msg[msg_cnt++] = MAKE_TS_MSG(l->op_id, l->ts);
							struct dep_entry *new_dst_entry = plmalloc_alloc(p, sizeof(struct dep_entry));
							memcpy(new_dst_entry->msg, &add_msg[msg_cnt - 2], 2 * sizeof(uint64_t));
							LIST_INSERT_AFTER(new_src_entry, new_dst_entry, deps);
							added_dependencies += 2;
						}
					}
				}
				// since this is a writer, exit the loop
				break;
			}
		}

		if ((have_readers_after) && (lq != NULL)) {
			int following_readers = 0;
			struct lock_entry *nxt = TAILQ_NEXT(to_remove, next);
			while ((nxt != NULL) && (nxt->optype == OPTYPE_LOOKUP)) {
				following_readers++;
				nxt = TAILQ_NEXT(nxt, next);
			}
			if (following_readers) {
				uint64_t add_msg[2 * following_readers + 2];
				int msg_cnt = 0;
				add_msg[msg_cnt++] = MAKE_HASH_MSG(lq->task_id - 2, lq->s, (unsigned long) e, DL_DETECT_ADD_DEP_SRC);
				add_msg[msg_cnt++] = MAKE_TS_MSG(lq->op_id, lq->ts);
				struct dep_entry *new_src_entry = plmalloc_alloc(p, sizeof(struct dep_entry));
				memcpy(new_src_entry->msg, &add_msg[0], 2 * sizeof(uint64_t));
				LIST_INSERT_HEAD(&dependencies, new_src_entry, deps);
				added_dependencies += 2;
				nxt = TAILQ_NEXT(to_remove, next);
				while ((nxt != NULL) && (nxt->optype == OPTYPE_LOOKUP)) {
					assert(not_two_readers_ww(lq, nxt));
					add_msg[msg_cnt++] = MAKE_HASH_MSG(nxt->task_id - 2, nxt->s, (unsigned long) e, DL_DETECT_ADD_DEP_TRG);
					add_msg[msg_cnt++] = MAKE_TS_MSG(nxt->op_id, nxt->ts);
					struct dep_entry *new_dst_entry = plmalloc_alloc(p, sizeof(struct dep_entry));
					memcpy(new_dst_entry->msg, &add_msg[msg_cnt - 2], 2 * sizeof(uint64_t));
					LIST_INSERT_AFTER(new_src_entry, new_dst_entry, deps);
					added_dependencies += 2;
					nxt = TAILQ_NEXT(nxt, next);
				}
			}
		}

		/* If (a) we have readers before the writer, (b) the writer to remove is the last in the waiters list
		 * and (c) all owners are readers, then the readers before the writer will get scheduled.
		 * So, if there is a writer before these readers, it must add a dependency to the existing owners.
		 */
		if ((have_readers) && (TAILQ_NEXT(to_remove, next) == NULL)) {
			int readers_owners = 0;
			struct lock_entry *owners_le;
			TAILQ_FOREACH(owners_le, &e->owners, next) {
				if (owners_le->optype == OPTYPE_LOOKUP) {
					readers_owners++;
				}
			}
			if (readers_owners) {
				// find the writer before the readers
				lq = TAILQ_PREV(to_remove, lock_tail, next);
				while ((lq != NULL) && (lq->optype == OPTYPE_LOOKUP)) {
					lq = TAILQ_PREV(lq, lock_tail, next);
				}
				if (lq != NULL) {
					uint64_t add_msg[2 * readers_owners + 2];
					int msg_cnt = 0;
					add_msg[msg_cnt++] = MAKE_HASH_MSG(lq->task_id - 2, lq->s, (unsigned long) e, DL_DETECT_ADD_DEP_SRC);
					add_msg[msg_cnt++] = MAKE_TS_MSG(lq->op_id, lq->ts);
					struct dep_entry *new_src_entry = plmalloc_alloc(p, sizeof(struct dep_entry));
					memcpy(new_src_entry->msg, &add_msg[0], 2 * sizeof(uint64_t));
					LIST_INSERT_HEAD(&dependencies, new_src_entry, deps);
					added_dependencies += 2;
					TAILQ_FOREACH(owners_le, &e->owners, next) {
						assert(not_two_readers_wo(lq, owners_le));
						add_msg[msg_cnt++] = MAKE_HASH_MSG(owners_le->task_id - 2, owners_le->s, (unsigned long) e, DL_DETECT_ADD_DEP_TRG);
						add_msg[msg_cnt++] = MAKE_TS_MSG(owners_le->op_id, owners_le->ts);
						struct dep_entry *new_dst_entry = plmalloc_alloc(p, sizeof(struct dep_entry));
						memcpy(new_dst_entry->msg, &add_msg[msg_cnt - 2], 2 * sizeof(uint64_t));
						LIST_INSERT_AFTER(new_src_entry, new_dst_entry, deps);
						added_dependencies += 2;
					}
				}
			}
		}
	} // Removed writer
	else {
		assert(0);
	}

	struct dep_entry *de, *de1;
	uint64_t deps_msg[added_dependencies + cleared_dependencies];
	int deps_msg_cnt = 0;
	LIST_FOREACH(de, &clr_dependencies, deps) {
		memcpy(&deps_msg[deps_msg_cnt], de->msg, 2 * sizeof(uint64_t));
		deps_msg_cnt += 2;
	}
	LIST_FOREACH(de, &dependencies, deps) {
		memcpy(&deps_msg[deps_msg_cnt], de->msg, 2 * sizeof(uint64_t));
		deps_msg_cnt += 2;
	}
	ring_buffer_write_all(&boxes[g_nservers - 1].boxes[s].out, deps_msg_cnt, deps_msg, 1);

	de = LIST_FIRST(&clr_dependencies);
	while (de != NULL) {
		de1 = LIST_NEXT(de, deps);
		LIST_REMOVE(de, deps);
		plmalloc_free(p, de, sizeof(struct dep_entry));
		de = de1;
	}

	de = LIST_FIRST(&dependencies);
	while (de != NULL) {
		de1 = LIST_NEXT(de, deps);
		LIST_REMOVE(de, deps);
		plmalloc_free(p, de, sizeof(struct dep_entry));
		de = de1;
	}
}

int dl_detect_check_acquire(struct elem *e, char optype)
{
  int r;

  if (optype == OPTYPE_LOOKUP) {
    if (!is_value_ready(e) || !(TAILQ_EMPTY(&e->waiters))) {
      r = LOCK_WAIT;
    } else {
      r = LOCK_SUCCESS;
    }
  } else {
    assert(optype == OPTYPE_UPDATE || optype == OPTYPE_INSERT);
    if (!is_value_ready(e) || e->ref_count > 1) {
      r = LOCK_WAIT;
    } else {
      r = LOCK_SUCCESS;
    }
  }

  return r;

}

int dl_detect_acquire(int s, struct partition *p,
    int c, int task_id, int op_id, struct elem *e, char optype,
    struct lock_entry **pl, uint64_t ts, int *notification) {

	dprint("srv(%d): In DL-DETECT Acquire for srv %d tid %d op_id %d\n", s, c, task_id, op_id);

	struct lock_entry *l;
	int r, conflict;

	dprint("srv(%d): cl %d %" PRIu64 " rc %" PRIu64 "\n", s, c, e->key, e->ref_count);

	if (optype == OPTYPE_LOOKUP) {
		conflict = !is_value_ready(e);
	} else {
		assert(optype == OPTYPE_UPDATE || optype == OPTYPE_INSERT);
		conflict = (!is_value_ready(e)) || (e->ref_count > 1);
	}

	// if there is a conflict or if there is already a waiter, then add to lock list
	if (conflict || !(TAILQ_EMPTY(&e->waiters))) {
		struct lock_entry *target = plmalloc_alloc(p, sizeof(struct lock_entry));
		assert(target);

		target->s = c;
		target->optype = optype;
		target->ready = 0;
		target->task_id = task_id;
		target->op_id = op_id;
		target->ts = ts;
		target->notify = notification;
		*pl = target;

		TAILQ_INSERT_HEAD(&e->waiters, target, next);
		int deadlock = add_dependencies(s, p, target, e);
		if (deadlock) {
			TAILQ_REMOVE(&e->waiters, target, next);
			r = LOCK_ABORT;
		} else {
			r = LOCK_WAIT;
		}

	} else {

		if (optype == OPTYPE_LOOKUP)
		e->ref_count++;
		else
		e->ref_count = DATA_READY_MASK | 2;

		// insert a lock in the owners list
		struct lock_entry *target = plmalloc_alloc(p, sizeof(struct lock_entry));
		assert(target);
		target->s = c;
		target->optype = optype;
		target->ready = 1;
		target->task_id = task_id;
		target->op_id = op_id;
		target->ts = ts;
		target->notify = notification;
		*pl = target;

		dprint("srv(%d): cl %d %" PRIu64 " rc %" PRIu64" adding to owners\n", s, c, e->key, e->ref_count);

		TAILQ_INSERT_HEAD(&e->owners, target, next);

		r = LOCK_SUCCESS;
	}

	return r;
}

void dl_detect_release(int s, struct partition *p, int c, int task_id,
    int op_id, struct elem *e, int notify)
{
	if (notify) {
		dprint("srv(%d): In DL-DETECT Release for srv %d tid %d key %d\n", s, c, task_id, e->key);
	} else {
		dprint("srv(%d): DL-DETECT Release for srv %d tid %d key %d called after DEADLOCK\n", s, c, task_id, e->key);
	}


	int owner_optype = -1;

	// find out lock on the owners list
	struct lock_entry *l;
	struct lock_entry *lq;
	struct lock_entry *prv_to_l = NULL;
	TAILQ_FOREACH(l, &e->owners, next) {
		dprint("srv(%d)-DL-DETECT_Release: Checking owner srv %d tid %d optype %d\n", s, l->s, l->task_id, l->optype);
		if (l->s == c && l->task_id == task_id) {
			dprint("srv(%d)-DL-DETECT_Release: Got srv %d tid %d in owners\n", s, l->s, l->task_id);
			break;
		}
	}

	if (l) {
		// free lock
		dprint("srv(%d): cl %d key %" PRIu64 " rc %" PRIu64" being removed from owners\n", s, c, e->key, e->ref_count);
		owner_optype = l->optype;

		TAILQ_REMOVE(&e->owners, l, next);
		plmalloc_free(p, l, sizeof(struct lock_entry));
		mp_release_value_(p, e);
	} else {
		// it is possible that lock is on waiters list. Imagine a scenario
		// where we send requests in bulk to 2 servers, one waits and other
		// aborts. In this case, we will get release message for a request
		// that is currently waiting
		TAILQ_FOREACH(lq, &e->waiters, next) {
			dprint("srv(%d)-DL-DETECT_Release: Checking waiter srv %d tid %d optype %d\n", s, lq->s, lq->task_id, lq->optype);
			if (lq->s == c && lq->task_id == task_id) {
				dprint("srv(%d)-DL-DETECT_Release: Got srv %d tid %d in waiters\n", s, lq->s, lq->task_id);
				break;
			}
		}
#if DEBUG
		if (!notify) {
			struct lock_entry *dle;
			TAILQ_FOREACH(dle, &e->owners, next) {
				dprint("srv(%d)-DL-DETECT_Release: Have srv %d tid %d optype %d in owners\n", s, dle->s, dle->task_id, dle->optype);
			}
			TAILQ_FOREACH(dle, &e->waiters, next) {
				dprint("srv(%d)-DL-DETECT_Release: Have srv %d tid %d optype %d in waiters\n", s, dle->s, dle->task_id, dle->optype);
			}
		}
#endif
		// can't be on neither owners nor waiters!
		assert(lq);
//		if (!notify)
//			update_dependencies(s, p, lq, e);
		TAILQ_REMOVE(&e->waiters, lq, next);
		plmalloc_free(p, lq, sizeof(struct lock_entry));
	}


	lq = TAILQ_LAST(&e->waiters, lock_tail);
	if (!lq) {
		return;
	}

	while (!TAILQ_EMPTY(&e->waiters)) {
		lq = TAILQ_LAST(&e->waiters, lock_tail);
		char conflict = 0;
		assert(lq);
		if (lq->optype == OPTYPE_LOOKUP) {
			conflict = !is_value_ready(e);
		} else {
			assert(lq->optype == OPTYPE_UPDATE || lq->optype == OPTYPE_INSERT);
			conflict = (!is_value_ready(e)) || (e->ref_count > 1);
		}

		if (!conflict) {
			/* there's no conflict only if there is a shared lock and we're
			* requesting a shared lock, or if there's no lock
			*/
			assert((e->ref_count & DATA_READY_MASK) == 0);

			if (lq->optype == OPTYPE_LOOKUP) {
				assert((e->ref_count & ~DATA_READY_MASK) >= 1);
				e->ref_count++;
			} else {
				assert((e->ref_count & ~DATA_READY_MASK) == 1);
				e->ref_count = DATA_READY_MASK | 2;
			}
		} else {
			break;
		}

		struct lock_entry *target = plmalloc_alloc(p, sizeof(struct lock_entry));
		assert(target);
		target->s = lq->s;
		target->optype = lq->optype;
		target->ready = lq->ready;
		target->task_id = lq->task_id;
		target->op_id = lq->op_id;
		target->ts = lq->ts;
		target->notify = lq->notify;

		TAILQ_INSERT_HEAD(&e->owners, target, next);

		TAILQ_REMOVE(&e->waiters, lq, next);
		plmalloc_free(p, lq, sizeof(struct lock_entry));


		// mark as ready and send message to server
		target->ready = 1;
		*(target->notify) = 1;

		mp_send_reply(s, target->s, target->task_id, target->op_id, e);

		dprint("srv(%d): release lock request for key %"PRIu64" marking %d as ready\n", s, e->key, target->s);
	}

	dprint("srv(%d)-DL-DETECT_Release: FINISHED\n", s);
}
#endif
