/*
 * dl_detect_graph.c
 *
 *  Created on: Mar 2, 2017
 *      Author: anadioti
 */
#if ENABLE_DL_DETECT_CC

#include <stdio.h>
#include <sys/queue.h>
#include <assert.h>
#include <string.h>

#include "dl_detect_graph.h"
#include "glo.h"
#include "util.h"
#include "type.h"
#include "util.h"
#include "hashprotocol.h"


#define __STDC_FORMAT_MACROS 1
#include <inttypes.h>

// forward declaration
void dl_detect_graph_clear_all_dependencies(int idx, uint64_t ts);

void dl_detect_graph_init() {
	nodes_index = (struct hash_node_list *) calloc(NODES_INDEX_SIZE * g_nservers * g_batch_size, sizeof(struct hash_node_list));
	for (int i = 0; i < g_nservers * g_batch_size; i ++) {
	  LIST_INIT(&nodes_index[i]);
	}
}

int get_index(struct node txn_node) {
	int index = (txn_node.srv * g_batch_size + txn_node.fib) * NODES_INDEX_SIZE + txn_node.ts % NODES_INDEX_SIZE;
//	int index = (txn_node.srv * g_batch_size + txn_node.fib);
	return index;
}

int nodes_equal(struct node n1, struct node n2) {
	return (n1.fib == n2.fib) && (n1.srv == n2.srv) && (n1.ts == n2.ts);
}

int node_in_list(struct node *n, struct node_q_head *list) {
	int ret = 0;
	struct node_q_entry *e;
	LIST_FOREACH(e, list, node_q_entries) {
		if (nodes_equal(e->txn_node, *n)) {
			ret = 1;
			break;
		}
	}

	return ret;
}

struct elem *get_waiting_entry(struct node *n) {
	struct elem *ret = NULL;

	struct waiting_elements_entry *entry;
	int waiters = 0;
	while (waiters != 1) {
		waiters = 0;
		LIST_FOREACH(entry, &n->wel, nxt_waiting_elem) {
			struct lock_tail_entry *l;
			TAILQ_FOREACH(l, &entry->e->waiters, next) {
				if ((l->s == n->srv) && (l->task_id == n->fib + 2) && (l->ts == n->ts)) {
					waiters ++;
					ret = entry->e;
					n->sender_srv = entry->sender;
				}
			}
		}
//		if (waiters > 1) {
//			LIST_FOREACH(entry, &n->wel, nxt_waiting_elem) {
//				struct lock_tail_entry *l;
//				TAILQ_FOREACH(l, &entry->e->waiters, next) {
//					if ((l->s == n->srv) && (l->task_id == n->fib + 2) && (l->ts == n->ts)) {
//						dprint("DL DETECT --- Node (%d,%d,%ld) is a waiter for key %ld\n", n->srv, n->fib, n->ts, entry->e->key);
//					}
//				}
//			}
//			assert(0);
//		}
//		dprint("Have %d waiters in the list\n", waiters);
	}

	return ret;
}

struct node *get_waiter(struct node *owner) {
	int idx = get_index(*owner);
	struct hash_node_list_entry *le;
	struct node *n = NULL;
	LIST_FOREACH(le, &nodes_index[idx], next_hash_node_list_entry) {
		if (LIST_FIRST(le->nodes)->txn_node.ts == owner->ts) {
			n = &LIST_FIRST(le->nodes)->txn_node;
			dprint("DL DETECT --- Waiter of node (%d,%d,%ld) is for key %ld\n", n->srv, n->fib, n->ts, ((struct elem *)(n->elem_addr_to_add))->key);
			break;
		}
	}

	return n;
}

struct node_q_entry *get_waiter_entry(struct node_q_entry *current_entry) {
	int idx = get_index(current_entry->txn_node);
	struct hash_node_list_entry *le;
	struct node_q_entry *n = NULL;
	LIST_FOREACH(le, &nodes_index[idx], next_hash_node_list_entry) {
		if (LIST_FIRST(le->nodes)->txn_node.ts == current_entry->txn_node.ts) {
			n = LIST_FIRST(le->nodes);
//			dprint("DL DETECT --- Waiter of node (%d,%d,%ld) is for key %ld\n", n->srv, n->fib, n->ts, ((struct elem *)(n->elem_addr))->key);
			break;
		}
	}

	return n;
}

int add_elem_not_exists(struct elem *e, int sender_srv, struct waiting_elements_list *list) {
	int added = 0;

	int found = 0;
	struct waiting_elements_entry *entry;
	LIST_FOREACH(entry, list, nxt_waiting_elem) {
		if (entry->e->key == e->key) {
			found = 1;
			break;
		}
	}

	if (!found) {
		struct waiting_elements_entry *we = (struct waiting_elements_entry *) malloc(sizeof(struct waiting_elements_entry));
		we->e = e;
		we->sender = sender_srv;
		LIST_INSERT_HEAD(list, we, nxt_waiting_elem);
		added = 1;
	}
	return added;
}

//int add_elem_not_exists(struct elem *e, int sender_srv, struct waiting_elements_list *list) {
//	int added = 0;
//
//	int found = 0;
//	struct waiting_elements_entry *entry = LIST_FIRST(list);
//	struct waiting_elements_entry *last_entry = entry;
//	while ((entry != NULL) && (entry->e->key < e->key)) {
//		entry = LIST_NEXT(entry, nxt_waiting_elem);
//		if (entry != NULL) {
//			last_entry = entry;
//		}
//	}
//
//	if (entry) {
//		if (entry->e->key > e->key) {
//			struct waiting_elements_entry *we = (struct waiting_elements_entry *) malloc(sizeof(struct waiting_elements_entry));
//			we->e = e;
//			we->sender = sender_srv;
//			LIST_INSERT_BEFORE(entry, we, nxt_waiting_elem);
//			added = 1;
//		}
//	} else {
//		if (!last_entry) {
//			struct waiting_elements_entry *we = (struct waiting_elements_entry *) malloc(sizeof(struct waiting_elements_entry));
//			we->e = e;
//			we->sender = sender_srv;
//			LIST_INSERT_HEAD(list, we, nxt_waiting_elem);
//			added = 1;
//		} else {
//			if (last_entry->e->key != e->key) {
//				struct waiting_elements_entry *we = (struct waiting_elements_entry *) malloc(sizeof(struct waiting_elements_entry));
//				we->e = e;
//				we->sender = sender_srv;
//				LIST_INSERT_AFTER(last_entry, we, nxt_waiting_elem);
//				added = 1;
//			}
//		}
//	}
//
//	return added;
//}

void dl_detect_graph_add_dependency(struct node src_node, struct node *trg_nodes, int nof_targets) {
	int idx = get_index(src_node);
	struct node_q_head *nodes_queue;
	struct hash_node_list_entry *le;
	int found = 0;
	LIST_FOREACH(le, &nodes_index[idx], next_hash_node_list_entry) {
//		if (TAILQ_FIRST(le->nodes)->txn_node.ts == src_node.ts) {
		if (LIST_FIRST(le->nodes)->txn_node.ts == src_node.ts) {
			nodes_queue = le->nodes;
			found = 1;
			break;
		}
	}
	if (!found) {
		dl_detect_graph_clear_all_dependencies(idx, src_node.ts);
		struct hash_node_list_entry *list_entry = (struct hash_node_list_entry *) malloc(sizeof(struct hash_node_list_entry));
		list_entry->nodes = (struct node_q_head *) malloc(sizeof(struct node_q_head));

//		TAILQ_INIT(list_entry->nodes);
		LIST_INIT(list_entry->nodes);

		LIST_INSERT_HEAD(&nodes_index[idx], list_entry, next_hash_node_list_entry);

		struct node_q_entry *entry = (struct node_q_entry *) malloc(sizeof(struct node_q_entry));
		entry->txn_node.fib = src_node.fib;
		entry->txn_node.srv = src_node.srv;
		entry->txn_node.ts = src_node.ts;
		LIST_INIT(&entry->txn_node.wel);
		int added = add_elem_not_exists((struct elem *) src_node.elem_addr_to_add, src_node.sender_srv, &entry->txn_node.wel);
		if (added) {
			entry->txn_node.neighbors = 1;
		}
//		struct waiting_elements_entry *we = (struct waiting_elements_entry *) malloc(sizeof(struct waiting_elements_entry));
//		we->e = (struct elem *) src_node.elem_addr_to_add;
//		LIST_INSERT_HEAD(&entry->txn_node.wel, we, nxt_waiting_elem);
//		entry->txn_node.elem_addr = src_node.elem_addr;
		entry->txn_node.added_ts = get_sys_clock();
		entry->txn_node.sender_srv = src_node.sender_srv;
		entry->txn_node.visited = 0;

		nodes_queue = list_entry->nodes;

//		TAILQ_INSERT_TAIL(nodes_queue, entry, node_q_entries);
		LIST_INSERT_HEAD(nodes_queue, entry, node_q_entries);

		dprint("DL DETECT --- Queue initialized\n");
	} else {
		struct waiting_elements_entry *we = (struct waiting_elements_entry *) malloc(sizeof(struct waiting_elements_entry));
		we->e = (struct elem *) src_node.elem_addr_to_add;
		int added = add_elem_not_exists((struct elem *) src_node.elem_addr_to_add, src_node.sender_srv, &LIST_FIRST(nodes_queue)->txn_node.wel);
		if (added) {
			LIST_FIRST(nodes_queue)->txn_node.neighbors ++;
		}
//		LIST_FIRST(nodes_queue)->txn_node.elem_addr = src_node.elem_addr;
		LIST_FIRST(nodes_queue)->txn_node.sender_srv = src_node.sender_srv;
		LIST_FIRST(nodes_queue)->txn_node.visited = 0;
	}

	for (int i = 0; i < nof_targets; i ++) {
		struct node_q_entry *entry = (struct node_q_entry *) malloc(sizeof(struct node_q_entry));
		entry->txn_node.fib = trg_nodes[i].fib;
		entry->txn_node.srv = trg_nodes[i].srv;
		entry->txn_node.ts = trg_nodes[i].ts;
		LIST_INIT(&entry->txn_node.wel);
//		add_elem_not_exists((struct elem *) src_node.elem_addr_to_add, src_node.sender_srv, &entry->txn_node.wel);
//		struct waiting_elements_entry *we = (struct waiting_elements_entry *) malloc(sizeof(struct waiting_elements_entry));
//		we->e = (struct elem *) src_node.elem_addr_to_add;
//		LIST_INSERT_HEAD(&entry->txn_node.wel, we, nxt_waiting_elem);
//		entry->txn_node.elem_addr = trg_nodes[i].elem_addr;
		entry->txn_node.added_ts = get_sys_clock();
		entry->txn_node.sender_srv = trg_nodes[i].sender_srv;
		entry->txn_node.visited = 0;

//		TAILQ_INSERT_TAIL(nodes_queue, entry, node_q_entries);
		LIST_INSERT_AFTER(LIST_FIRST(nodes_queue), entry, node_q_entries);

		dprint("DL DETECT --- Inserting node (%d,%d,%"PRIu64")\n",
					entry->txn_node.srv, entry->txn_node.fib, entry->txn_node.ts);
	}
//	dprint("DL DETECT --- First node is (%d,%d,%ld)\n",
//			TAILQ_FIRST(nodes_queue)->txn_node.srv, TAILQ_FIRST(nodes_queue)->txn_node.fib, TAILQ_FIRST(nodes_queue)->txn_node.ts);
	dprint("DL DETECT --- First node is (%d,%d,%"PRIu64")\n",
				LIST_FIRST(nodes_queue)->txn_node.srv, LIST_FIRST(nodes_queue)->txn_node.fib, LIST_FIRST(nodes_queue)->txn_node.ts);
}

int next_node(struct node_q_entry *src, int *deadlock_flag, struct node *deadlock_node) {
	int ret = 0;

	if (*deadlock_flag == 1) {
		src->txn_node.visited = 0;
		return 1;
	}
	dprint("DL DETECT --- Checking node (%d,%d,%ld)\n", src->txn_node.srv, src->txn_node.fib, src->txn_node.ts);

	if (src->txn_node.visited) {
		// if the node is visited, then we have a deadlock
		dprint("DL DETECT --- Deadlock in node (%d,%d,%ld)\n", src->txn_node.srv, src->txn_node.fib, src->txn_node.ts);
		src->txn_node.visited = 0;
		*deadlock_flag = 1;
		deadlock_node->added_ts = src->txn_node.added_ts;
		deadlock_node->elem_addr_to_add = (uint64_t) get_waiting_entry(&src->txn_node);
//		deadlock_node->elem_addr = src->txn_node.elem_addr;
		deadlock_node->fib = src->txn_node.fib;
		deadlock_node->sender_srv = src->txn_node.sender_srv;
		deadlock_node->srv = src->txn_node.srv;
		deadlock_node->ts = src->txn_node.ts;
		deadlock_node->visited = src->txn_node.visited;

		return 1;
	} else {
		src->txn_node.visited = 1;
		struct node_q_entry *nxt = LIST_NEXT(src, node_q_entries);
		while (nxt != NULL) {
			struct node_q_entry *waiter_entry = get_waiter_entry(nxt);
			dprint("DL DETECT --- Trying node (%d,%d,%ld)\n",
						nxt->txn_node.srv, nxt->txn_node.fib, nxt->txn_node.ts);
			if (waiter_entry != NULL) {
//				printf("DL DETECT --- Waiter (%d,%d,%ld) has %d neigbhors\n",
//						waiter_entry->txn_node.srv, waiter_entry->txn_node.fib, waiter_entry->txn_node.ts, waiter_entry->txn_node.neighbors);
				ret = next_node(waiter_entry, deadlock_flag, deadlock_node);
				waiter_entry->txn_node.visited = 0;
				if (ret == 1) {
					return 1;
				}
			}

			nxt = LIST_NEXT(nxt, node_q_entries);
		}

	}

	return ret;
}

#if 0
int collect_nodes(struct node *rec_first_node, struct node_q_head *blacklisted,
		struct node_q_head *collected_nodes, int *deadlock_flag, struct node *deadlock_node) {
	if (*deadlock_flag == 1) {
		return 1;
	}
	int ret = 0;

	struct node_q_entry *collected_nodes_entry;
	LIST_FOREACH(collected_nodes_entry, collected_nodes, node_q_entries) {
		dprint("DL DETECT --- Checking nodes (%d,%d,%ld) with (%d,%d,%ld)\n",
				rec_first_node->srv, rec_first_node->fib, rec_first_node->ts,
				collected_nodes_entry->txn_node.srv, collected_nodes_entry->txn_node.fib, collected_nodes_entry->txn_node.ts);
		if (nodes_equal(*rec_first_node, collected_nodes_entry->txn_node)) {
			struct node *deadlock_waiter = get_waiter(&collected_nodes_entry->txn_node);
			int waiter_found = 0;
//			struct lock_entry *le;
//			LIST_FOREACH(le, &((struct elem *)(deadlock_waiter->elem_addr))->waiters, next) {
			struct lock_tail_entry *le;
			TAILQ_FOREACH(le, &((struct elem *)(deadlock_waiter->elem_addr))->waiters, next) {
				if ((le->s == deadlock_waiter->srv) &&
						(le->task_id == deadlock_waiter->fib + 2) &&
						(le->ts == deadlock_waiter->ts)) {
					waiter_found = 1;
					break;
				}
			}

//			assert(waiter_found);
			if (waiter_found) {
				*deadlock_flag = 1;

				deadlock_node->added_ts = deadlock_waiter->added_ts;
				deadlock_node->elem_addr = deadlock_waiter->elem_addr;
				deadlock_node->fib = deadlock_waiter->fib;
				deadlock_node->sender_srv = deadlock_waiter->sender_srv;
				deadlock_node->srv = deadlock_waiter->srv;
				deadlock_node->ts = deadlock_waiter->ts;

				return 1;
			} else {
				struct node_q_entry *be = (struct node_q_entry *) malloc(sizeof(struct node_q_entry));
				be->txn_node.added_ts = deadlock_waiter->added_ts;
				be->txn_node.elem_addr = deadlock_waiter->elem_addr;
				be->txn_node.fib = deadlock_waiter->fib;
				be->txn_node.sender_srv = deadlock_waiter->sender_srv;
				be->txn_node.srv = deadlock_waiter->srv;
				be->txn_node.ts = deadlock_waiter->ts;
				LIST_INSERT_HEAD(blacklisted, be, node_q_entries);
			}
		}
	}
	assert(*deadlock_flag != 1);

	// get the next node in the queue
	int idx = get_index(*rec_first_node);
	struct hash_node_list_entry *le;
	struct node_q_head *nodes_queue;
	int found = 0;
	LIST_FOREACH(le, &nodes_index[idx], next_hash_node_list_entry) {
		if (nodes_equal(LIST_FIRST(le->nodes)->txn_node, *rec_first_node)) {
			nodes_queue = le->nodes;
			found = 1;
			break;
		}
	}
	if (found) {
		// add the current node to the collected ones
		struct node_q_entry *nqe = (struct node_q_entry *) malloc(sizeof(struct node_q_entry));
		nqe->txn_node.added_ts = rec_first_node->added_ts;
		nqe->txn_node.elem_addr = rec_first_node->elem_addr;
		nqe->txn_node.fib = rec_first_node->fib;
		nqe->txn_node.sender_srv = rec_first_node->sender_srv;
		nqe->txn_node.srv = rec_first_node->srv;
		nqe->txn_node.ts = rec_first_node->ts;
		LIST_INSERT_HEAD(collected_nodes, nqe, node_q_entries);
		// call the next check recurrently for the next node in the queue
		struct node_q_entry *e;
		LIST_FOREACH(e, nodes_queue, node_q_entries) {
			if ((!nodes_equal(e->txn_node, *rec_first_node)) &&
					(!node_in_list(&e->txn_node, blacklisted))) {
				ret = collect_nodes(&e->txn_node, blacklisted, collected_nodes, deadlock_flag, deadlock_node);
				if (ret == 1) {
					break;
				}
			}
		}
		LIST_REMOVE(nqe, node_q_entries);
		free(nqe);
		return ret;
	} else {
		return 0;
	}

	return ret;
}
//#if 0
int collect_nodes(struct node src_node, struct node rec_first_node,
		struct node_q_head *collected_nodes, int *deadlock_flag, struct node *deadlock_node) {
	int ret = 1;
	dprint("DL DETECT --- Checking node (%d,%d,%"PRIu64")\n",
					rec_first_node.srv, rec_first_node.fib, rec_first_node.ts);
	int idx = get_index(rec_first_node);
	struct node_q_head *nodes_queue;
	struct hash_node_list_entry *le;
	int found = 0;
	LIST_FOREACH(le, &nodes_index[idx], next_hash_node_list_entry) {
		if (nodes_equal(LIST_FIRST(le->nodes)->txn_node, rec_first_node)) {
			nodes_queue = le->nodes;
			found = 1;
			break;
		}
	}

	if (found) {
		struct node_q_entry *e;
		LIST_FOREACH(e, nodes_queue, node_q_entries) {
//			dprint("DL DETECT --- In loop of node (%d,%d,%"PRIu64")\n",
//					rec_first_node.srv, rec_first_node.fib, rec_first_node.ts);
			if ((*deadlock_flag != 1) && (!nodes_equal(rec_first_node, e->txn_node))) {
				dprint("DL DETECT --- Collecting node (%d,%d,%"PRIu64")\n",
							e->txn_node.srv, e->txn_node.fib, e->txn_node.ts);
				int collect = 1;
				if (nodes_equal(src_node, e->txn_node)) {
					dprint("DL DETECT --- Found the DEADLOCK in SRC\n");

					int waiter_found = 0;
					struct node *n = get_waiter(&e->txn_node);
					if (n != NULL) {
//								waiter_found = 1;
						deadlock_node->elem_addr = n->elem_addr;
						deadlock_node->sender_srv = n->sender_srv;
						struct lock_entry *le;
						LIST_FOREACH(le, &((struct elem *)(n->elem_addr))->waiters, next) {
							if ((le->s == n->srv) &&
									((le->task_id - 2) == n->fib) &&
									(le->ts == n->ts)) {
								dprint("Found indeed a WAITER\n");
								waiter_found = 1;
								break;
							} else {
								dprint("Failed with waiter (%d,%d,%ld)\n", le->s, le->task_id, le->ts);
							}
						}
//								assert(found);
					}

					if (waiter_found) {
						*deadlock_flag = 1;
						deadlock_node->elem_addr = src_node.elem_addr;
						deadlock_node->fib = e->txn_node.fib;
						deadlock_node->srv = e->txn_node.srv;
						deadlock_node->ts = e->txn_node.ts;
//						deadlock_node->sender_srv = e->txn_node.sender_srv;

						dprint("DL DETECT --- Setting the deadlock node to src (%d,%d,%ld) KEY %ld\n",
									deadlock_node->srv, deadlock_node->fib, deadlock_node->ts, ((struct elem *)(deadlock_node->elem_addr))->key);
					}
				} else {
					struct node_q_entry *ne;

					LIST_FOREACH(ne, collected_nodes, node_q_entries) {
						if (nodes_equal(e->txn_node, ne->txn_node)) {
							dprint("DL DETECT --- Found POSSIBLE DEADLOCK in COLLECTION\n");

							int waiter_found = 0;
							struct node *n = get_waiter(&e->txn_node);
							if (n != NULL) {
//								waiter_found = 1;
								deadlock_node->elem_addr = n->elem_addr;
								deadlock_node->sender_srv = n->sender_srv;
								struct lock_entry *le;
								int found = 0;
								LIST_FOREACH(le, &((struct elem *)(n->elem_addr))->waiters, next) {
									if ((le->s == n->srv) &&
											((le->task_id - 2) == n->fib) &&
											(le->ts == n->ts)) {
										dprint("Found indeed a WAITER\n");
										found = 1;
										waiter_found = 1;
										break;
									} else {
										dprint("Failed with waiter (%d,%d,%ld)\n", le->s, le->task_id, le->ts);
									}
								}
//								assert(found);
							}

//							assert(waiter_found);

							if (waiter_found) {
								*deadlock_flag = 1;
								deadlock_node->fib = n->fib;
								deadlock_node->srv = n->srv;
								deadlock_node->ts = n->ts;
//								deadlock_node->sender_srv = n->sender_srv;
								dprint("DL DETECT --- Setting the deadlock node to collection (%d,%d,%ld) KEY %ld\n",
											deadlock_node->srv, deadlock_node->fib, deadlock_node->ts, ((struct elem *)(deadlock_node->elem_addr))->key);
								break;
							} else {
								dprint("Nodes (%d,%d,%ld) and (%d,%d,%ld) are not waiters\n",
										e->txn_node.srv, e->txn_node.fib, e->txn_node.ts,
										ne->txn_node.srv, ne->txn_node.fib, ne->txn_node.ts);
								collect = 0;
								break;
							}

//							break;
						}
					}
					if (!collect) {
						LIST_REMOVE(ne, node_q_entries);
						free(ne);
					}
				}
				struct node_q_entry *entry = (struct node_q_entry *) malloc(sizeof(struct node_q_entry));
				if (collect) {
					entry->txn_node.added_ts = e->txn_node.added_ts;
					entry->txn_node.elem_addr = e->txn_node.elem_addr;
					entry->txn_node.fib = e->txn_node.fib;
					entry->txn_node.srv = e->txn_node.srv;
					entry->txn_node.ts = e->txn_node.ts;
					entry->txn_node.sender_srv = e->txn_node.sender_srv;
					LIST_INSERT_HEAD(collected_nodes, entry, node_q_entries);
				}

				if (*deadlock_flag != 1) {
					ret = collect_nodes(src_node, e->txn_node, collected_nodes, deadlock_flag, deadlock_node);
					dprint("DL DETECT --- Removing node from collection (%d,%d,%ld) KEY %ld\n",
								entry->txn_node.srv, entry->txn_node.fib, entry->txn_node.ts, ((struct elem *)(entry->txn_node.elem_addr))->key);
					if (collect) {
						LIST_REMOVE(entry, node_q_entries);
						free(entry);
					}

				} else {
					return 1;
				}
			}
		}
	} else {
		dprint("DL DETECT --- No queue found for node (%d,%d,%"PRIu64")\n",
					rec_first_node.srv, rec_first_node.fib, rec_first_node.ts);
	}

	return ret;
}
#endif
/*
int collect_nodes(struct node src_node, struct node rec_first_node,
		struct node_q_head *collected_nodes, int *deadlock_flag, struct node *deadlock_node) {
	int ret = 1;
	dprint("DL DETECT --- In collect nodes\n");
	if (*deadlock_flag != 1) {
		ret = 0;
		dprint("DL DETECT --- Checking nod20092e (%d,%d,%"PRIu64")\n",
				rec_first_node.srv, rec_first_node.fib, rec_first_node.ts);
		int idx = get_index(rec_first_node);
		struct node_q_head *nodes_queue;
		struct hash_node_list_entry *le;
		int found = 0;
		LIST_FOREACH(le, &nodes_index[idx], next_hash_node_list_entry) {
	//		if (TAILQ_FIRST(le->nodes)->txn_node.ts == rec_first_node.ts) {
			if (LIST_FIRST(le->nodes)->txn_node.ts == rec_first_node.ts) {
				nodes_queue = le->nodes;
				found = 1;
				dprint("DL DETECT --- Found a queue\n");
				break;
			}
		}
		if (found) {
			struct node_q_entry *e;
	//		TAILQ_FOREACH(e, nodes_queue, node_q_entries) {
			LIST_FOREACH(e, nodes_queue, node_q_entries) {
				if (*deadlock_flag == 1) {
					return 1;
				} else {
					if (!nodes_equal(e->txn_node, rec_first_node)) {
						dprint("DL DETECT --- Collecting node (%d,%d,%"PRIu64")\n",
								e->txn_node.srv, e->txn_node.fib, e->txn_node.ts);

						if (nodes_equal(e->txn_node, src_node)) {
							dprint("DL DETECT --- Found the DEADLOCK\n");
							*deadlock_flag = 1;
							deadlock_node->fib = src_node.fib;
							deadlock_node->srv = src_node.srv;
							deadlock_node->ts = src_node.ts;
							deadlock_node->elem_addr = src_node.elem_addr;
							dprint("DL DETECT --- Setting the deadlock node to src (%d,%d,%ld) KEY %ld\n",
										deadlock_node->srv, deadlock_node->fib, deadlock_node->ts, ((struct elem *)(deadlock_node->elem_addr))->key);
							LIST_INSERT_HEAD(collected_nodes, e, node_q_entries);
							return 1;
						} else {
							struct node_q_entry *ne;
							LIST_FOREACH(ne, collected_nodes, node_q_entries) {
								if (nodes_equal(ne->txn_node, e->txn_node)) {
									*deadlock_flag = 1;
									deadlock_node->fib = e->txn_node.fib;
									deadlock_node->srv = e->txn_node.srv;
									deadlock_node->ts = e->txn_node.ts;
									deadlock_node->elem_addr = e->txn_node.elem_addr;
									ret = 1;
									dprint("DL DETECT --- Setting the deadlock node to (%d,%d,%ld) KEY %ld\n",
												deadlock_node->srv, deadlock_node->fib, deadlock_node->ts, ((struct elem *)(deadlock_node->elem_addr))->key);
									LIST_INSERT_HEAD(collected_nodes, e, node_q_entries);
									return 1;
//									break;
								}
							}
							LIST_INSERT_HEAD(collected_nodes, e, node_q_entries);
							if (*deadlock_flag != 1) {
								ret = collect_nodes(src_node, e->txn_node, collected_nodes, deadlock_flag, deadlock_node);
							}
						}
		//				TAILQ_INSERT_TAIL(nodes_collection, e, node_q_entries);
		//				LIST_INSERT_HEAD(nodes_collection, e, node_q_entries);
		//
		//				if ((e->txn_node.srv != src_node.srv ||
		//						e->txn_node.fib != src_node.fib ||
		//						e->txn_node.ts != src_node.ts)) {
		//					collect_nodes(src_node, e->txn_node, nodes_collection);
		//
		//				}
					} else {
						dprint("DL DETECT --- NOT Collecting node (%d,%d,%"PRIu64")\n",
								e->txn_node.srv, e->txn_node.fib, e->txn_node.ts);
					}
				}
			}
		} else {
			dprint("DL DETECT --- NOT Found a queue\n");
		}
	}


	return ret;
}
*/
int dl_detect_graph_detect_cycle(struct node src_node, struct node *deadlock_node) {
	int ret = 0;
	struct node_q_head nodes_collection, blacklisted_nodes;
//	struct node deadlock_node;
	deadlock_node->fib = -1;
	deadlock_node->srv = -1;
	deadlock_node->ts = 0;
	deadlock_node->elem_addr_to_add = 0;
//	TAILQ_INIT(&nodes_collection);
	LIST_INIT(&nodes_collection);
	LIST_INIT(&blacklisted_nodes);
	int deadlock = 0;
	dprint("DL DETECT --- Checking for DEADLOCK from node (%d,%d,%ld)\n", src_node.srv, src_node.fib, src_node.ts);
//	fflush(stdout);
//	ret = collect_nodes(src_node, src_node, &nodes_collection, &deadlock, deadlock_node);

//	ret = collect_nodes(&src_node, &blacklisted_nodes, &nodes_collection, &deadlock, deadlock_node);
	// find the src node in the graph
	int idx = get_index(src_node);
	struct hash_node_list_entry *le;
	struct node_q_head *nodes_queue;
	int found = 0;
	LIST_FOREACH(le, &nodes_index[idx], next_hash_node_list_entry) {
		if (nodes_equal(LIST_FIRST(le->nodes)->txn_node, src_node)) {
			nodes_queue = le->nodes;
			found = 1;
			break;
		}
	}
	if (found) {
		ret = next_node(LIST_FIRST(nodes_queue), &deadlock, deadlock_node);
		LIST_FIRST(nodes_queue)->txn_node.visited = 0;
	}


	if (!deadlock) {
		dprint("DL DETECT --- No DEADLOCK\n");
	} else {
//		printf("DL DETECT --- DEADLOCK\n");
		dprint("DL DETECT --- DEADLOCK NODE (%d,%d,%ld) KEY %ld\n",
					deadlock_node->srv, deadlock_node->fib, deadlock_node->ts, ((struct elem *)(deadlock_node->elem_addr_to_add))->key);
	}
//	fflush(stdout);
//	struct node_q_entry *e;
////	TAILQ_FOREACH(e, &nodes_collection, node_q_entries) {
//	LIST_FOREACH(e, &nodes_collection, node_q_entries) {
//		if (e->txn_node.srv == src_node.srv &&
//				e->txn_node.fib == src_node.fib &&
//				e->txn_node.ts == src_node.ts) {
//			dprint("DL DETECT --- Checking node (%d,%d,%ld) with (%d,%d,%ld)\n",
//					src_node.srv, src_node.fib, src_node.ts,
//					e->txn_node.srv, e->txn_node.fib, e->txn_node.ts);
//
//			dprint("DL DETECT --- DEADLOCK\n");
//			return 1;
//		}
//	}
//
//	dprint("DL DETECT --- No DEADLOCK\n");
	return deadlock;
}

void dl_detect_graph_rmv_dependency(struct node *src, struct node *trg) {
	int idx = get_index(*src);
	struct hash_node_list_entry *le = LIST_FIRST(&nodes_index[idx]);
	struct hash_node_list_entry *le1;
	while (le != NULL) {
		le1 = LIST_NEXT(le, next_hash_node_list_entry);
		struct node_q_entry *ne = LIST_FIRST(le->nodes);
		if (nodes_equal(*src, ne->txn_node)) {
			ne = LIST_NEXT(ne, node_q_entries);
			struct node_q_entry *ne1;
			while (ne != NULL) {
				ne1 = LIST_NEXT(ne, node_q_entries);
				if (nodes_equal(*trg, ne->txn_node)) {
					LIST_REMOVE(ne, node_q_entries);
					free(ne);
				}
				ne = ne1;
			}
			break;
		}

		le = le1;
	}
}

void dl_detect_graph_clear_dependency(struct node to_remove) {
//	printf("DL DETECT --- Clearing deps for node (%d,%d,%ld)\n", to_remove.srv, to_remove.fib, to_remove.ts);
	int idx = get_index(to_remove);
#if 0
	// First remove all the chains starting with that node
	struct hash_node_list_entry* le = (struct hash_node_list_entry *) malloc(sizeof(struct hash_node_list_entry));
	struct hash_node_list_entry* le1 = (struct hash_node_list_entry *) malloc(sizeof(struct hash_node_list_entry));
	le = LIST_FIRST(&nodes_index[idx]);
	while (le != NULL) {
		le1 = LIST_NEXT(le, next_hash_node_list_entry);
		free(le);
		le = le1;
	}
	LIST_INIT(&nodes_index[idx]);
#endif

	struct hash_node_list_entry *le, *le1;
	le = LIST_FIRST(&nodes_index[idx]);
	while (le != NULL) {
		le1 = LIST_NEXT(le, next_hash_node_list_entry);
		struct node_q_entry *ne = LIST_FIRST(le->nodes);
		struct node_q_entry *ne1;
//		printf("In queue of node (%d,%d,%ld)\n", ne->txn_node.srv, ne->txn_node.fib, ne->txn_node.ts);
		if (nodes_equal(ne->txn_node, to_remove)) {
			while (ne != NULL) {
				ne1 = LIST_NEXT(ne, node_q_entries);
				dprint("DL DETECT --- Removing node (%d,%d,%ld)\n", ne->txn_node.srv, ne->txn_node.fib, ne->txn_node.ts);
				LIST_REMOVE(ne, node_q_entries);
				free(ne);
				ne = ne1;
			}
			if (le != NULL) {
				dprint("DL DETECT --- Still have the head node\n");
				LIST_REMOVE(le, next_hash_node_list_entry);
				free(le);
//				le = le1;
//				if (LIST_FIRST(le->nodes) != NULL) {
//					dprint("DL DETECT --- First node is (%d,%d,%ld)\n",
//							LIST_FIRST(le->nodes)->txn_node.srv, LIST_FIRST(le->nodes)->txn_node.fib, LIST_FIRST(le->nodes)->txn_node.ts);
//				}
			}
			break;
		}
		le = le1;
	}
	if (LIST_EMPTY(&nodes_index[idx])) {
		LIST_INIT(&nodes_index[idx]);
	}
}

void dl_detect_graph_clear_all_dependencies(int idx, uint64_t ts) {
	struct hash_node_list_entry *le, *le1;
	le = LIST_FIRST(&nodes_index[idx]);
	while (le != NULL) {
		le1 = LIST_NEXT(le, next_hash_node_list_entry);
		struct node_q_entry *ne = LIST_FIRST(le->nodes);
		if (ne->txn_node.ts < ts) {
			struct node_q_entry *ne1;
			while (ne != NULL) {
				ne1 = LIST_NEXT(ne, node_q_entries);
				free(ne);
				ne = ne1;
			}
			LIST_REMOVE(le, next_hash_node_list_entry);
			free(le);
		}

		le = le1;
	}

//	LIST_INIT(&nodes_index[idx]);
}

#endif //DL_DETECT_ENABLED
