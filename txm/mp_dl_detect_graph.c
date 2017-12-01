/*
 * mp_dl_detect_graph.c
 *
 *  Created on: Mar 16, 2017
 *      Author: anadioti
 */

#include <stdio.h>
#include <string.h>
#include <assert.h>
#include <stdlib.h>

#include "mp_dl_detect_graph.h"
#include "util.h"


int get_node_index(struct dl_detect_graph_node *node) {
	int idx = node->srv * g_nfibers + node->fib;

	return idx;
}

int nodes_equal(struct waiter_node *node1, struct waiter_node *node2) {
	int ret = 0;

	if ((node1->fib == node2->fib) &&
			(node1->srv == node2->srv) &&
			(node1->ts == node2->ts) &&
			(node1->opid == node2->opid)) {
		ret = 1;
	}

	return ret;
}

void mp_dl_detect_init_dependency_graph() {
	cycle_visited_nodes = (int *) malloc(g_nservers * g_nfibers * sizeof(int));
	cycle_remaining_nodes = (int *) malloc(g_nservers * g_nfibers * sizeof(int));
	cycle_added_nodes = (int *) malloc(g_nservers * g_nfibers * sizeof(int));

	graph_nodes = (struct dl_detect_graph_node *) malloc(g_nservers * g_nfibers * sizeof(struct dl_detect_graph_node));
	for (int i = 0; i < g_nservers * g_nfibers; i ++) {
		graph_nodes[i].neighbors = (struct waiter_node *) malloc(g_nservers * g_nfibers * sizeof(struct waiter_node));
	}
}

int mp_dl_detect_add_dependency(struct dl_detect_graph_node *src) {
	int idx = get_node_index(src);
	struct dl_detect_graph_node *cur = &graph_nodes[idx];
	if (cur->ts > src->ts) {	// out-of-order arrival
		dprint("DL DETECT --- Not adding: Found node with ts %ld - req node ts %ld\n", cur->ts, src->ts);
		return 0;
	} else if (cur->ts == src->ts) {	// arrival for an active txn
		if (cur->has_aborted) {
			return 0;
		}
		if (cur->opid > src->opid) {	// arrival for an old operation
			dprint("DL DETECT --- Not adding: Found node with opid %d - req node opid %d\n", cur->opid, src->opid);
			return 0;
		} else if (cur->opid == src->opid) {	// arrival for the current operation
			// add the targets
			memcpy(&cur->neighbors[cur->waiters_size], &src->neighbors[0], src->waiters_size * sizeof(struct waiter_node));
			cur->waiters_size += src->waiters_size;
			cur->e = src->e;
			cur->sender_srv = src->sender_srv;
		} else {	// arrival for a new operation
			// we have a new request; the old targets can be replaced
			cur->waiters_size = 0;
			// add the targets
			memcpy(&cur->neighbors[cur->waiters_size], &src->neighbors[0], src->waiters_size * sizeof(struct waiter_node));
			cur->waiters_size = src->waiters_size;
			// finally set the new element the txn is waiting for and the server that sent the new dependency
			cur->e = src->e;
			cur->sender_srv = src->sender_srv;
			cur->opid = src->opid;
		}
	} else {	// new txn arrival
		cur->e = src->e;
		cur->fib = src->fib;
		cur->opid = src->opid;
		cur->sender_srv = src->sender_srv;
		cur->srv = src->srv;
		cur->ts = src->ts;
		cur->visited = src->visited;
		cur->waiters_size = src->waiters_size;
		cur->has_aborted = 0;

		memcpy(cur->neighbors, src->neighbors, src->waiters_size * sizeof(struct waiter_node));
	}

#if DEBUG
	dprint("DL DETECT[ADD] --- Added deps from node (%d,%d,%ld)\n", src->srv, src->fib, src->ts);
	for (int i = 0; i < cur->waiters_size; i ++) {
		dprint("DL DETECT[ADD] --- Now have dependency: (%d,%d,%ld) ---> (%d,%d,%ld)\n",
				cur->srv, cur->fib, cur->ts,
				cur->neighbors[i].srv, cur->neighbors[i].fib, cur->neighbors[i].ts);
	}
#endif

	return 1;
}

int iterative_cycle_check(struct dl_detect_graph_node *src, struct dl_detect_graph_node *deadlock_node, int *deadlock_flag) {
	int ret = 0;

	int src_idx = src->srv * g_nfibers + src->fib;
	int idx = src->srv * g_nfibers + src->fib;

	int cycle_remaining_nodes_w_idx = 0;
	int cycle_remaining_nodes_r_idx = 0;
	memset(cycle_visited_nodes, 0, g_nfibers * g_nservers * sizeof(int));
	memset(cycle_added_nodes, 0, g_nfibers * g_nservers * sizeof(int));
	cycle_remaining_nodes[cycle_remaining_nodes_w_idx++] = idx;

	while (cycle_remaining_nodes_r_idx < cycle_remaining_nodes_w_idx) {
		idx = cycle_remaining_nodes[cycle_remaining_nodes_r_idx++];

		if (!cycle_visited_nodes[idx]) {
			cycle_visited_nodes[idx] = 1;
			for (int i = 0; i < graph_nodes[idx].waiters_size; i ++) {
				int waiter_index = graph_nodes[idx].neighbors[i].srv * g_nfibers + graph_nodes[idx].fib;
				if (graph_nodes[waiter_index].ts == graph_nodes[idx].neighbors[i].ts) {
					if (waiter_index == src_idx) {
						// DEADLOCK
						*deadlock_flag = 1;
						return 1;
					}
					if (!cycle_added_nodes[waiter_index]) {
						cycle_added_nodes[waiter_index] = 1;
						cycle_remaining_nodes[cycle_remaining_nodes_w_idx++] = waiter_index;
					}

				}
			}
		}
		assert(cycle_remaining_nodes_r_idx <= g_nservers * g_nfibers);
		assert(cycle_remaining_nodes_w_idx <= g_nservers * g_nfibers);
	}

	return ret;
}

#if RECURSIVE_CYCLE_CHECK
int next_node(struct dl_detect_graph_node *src, struct dl_detect_graph_node *deadlock_node, int *deadlock_flag) {
	int ret = 0;

	if (*deadlock_flag == 1) {
		src->visited = 0;
		return 1;
	}
	dprint("DL DETECT[CYCLE] --- Checking node (%d,%d,%ld)\n", src->srv, src->fib, src->ts);

	if (src->visited) {
		// if the node is visited, then we have a deadlock
		dprint("DL DETECT[CYCLE] --- Deadlock in node (%d,%d,%ld)\n", src->srv, src->fib, src->ts);
		src->visited = 0;
		*deadlock_flag = 1;

		deadlock_node->e = src->e;
		deadlock_node->fib = src->fib;
		deadlock_node->opid = src->opid;
		deadlock_node->sender_srv = src->sender_srv;
		deadlock_node->srv = src->srv;
		deadlock_node->ts = src->ts;
		deadlock_node->visited = src->visited;

		return 1;
	} else {
		src->visited = 1;

		for (int i = 0; i < src->waiters_size; i ++) {
			dprint("DL DETECT[CYCLE] --- Trying node (%d,%d,%ld)\n", src->neighbors[i].srv, src->neighbors[i].fib, src->neighbors[i].ts);
			int idx = src->neighbors[i].srv * g_nfibers + src->neighbors[i].fib;
			if (src->neighbors[i].ts == graph_nodes[idx].ts) {
				ret = next_node(&graph_nodes[idx], deadlock_node, deadlock_flag);
				graph_nodes[idx].visited = 0;
				if (ret == 1) {
					return 1;
				}
			}
		}
	}

	return ret;
}
#endif

int mp_dl_detect_detect_cycle(struct dl_detect_graph_node *src, struct dl_detect_graph_node *deadlock_node) {
	int idx = get_node_index(src);
	assert(graph_nodes[idx].ts == src->ts);
	int deadlock = 0;
	dprint("DL DETECT[CYCLE] --- Start cycle check for node (%d,%d,%ld)\n", src->srv, src->fib, src->ts);
#if RECURSIVE_CYCLE_CHECK
	int ret = next_node(&graph_nodes[idx], deadlock_node, &deadlock);
#else
	int ret = iterative_cycle_check(&graph_nodes[idx], deadlock_node, &deadlock);
	if (ret) {
		memcpy(deadlock_node, &graph_nodes[idx], sizeof(struct dl_detect_graph_node));
	}
#endif

#if DEBUG
	if (!deadlock) {
		dprint("DL DETECT[CYCLE] --- No DEADLOCK\n");
	} else {
		dprint("DL DETECT[CYCLE] --- DEADLOCK NODE (%d,%d,%ld) KEY %ld\n",
					deadlock_node->srv, deadlock_node->fib, deadlock_node->ts, deadlock_node->e->key);
	}
#endif
	graph_nodes[idx].visited = 0;

	return deadlock;
}

void mp_dl_detect_remove_dependency(struct dl_detect_graph_node *src) {
	int idx = get_node_index(src);
	if (idx != -1) {
		struct dl_detect_graph_node *cur = &graph_nodes[idx];
		for (int j = 0; j < src->waiters_size; j ++) {
			int i = 0;
			while (i < cur->waiters_size) {
				if (nodes_equal(&cur->neighbors[i], &src->neighbors[j])) {
					memmove(&cur->neighbors[i], &cur->neighbors[i + 1], (cur->waiters_size - i - 1) * sizeof(struct waiter_node));
					cur->waiters_size--;
					break;
				}
				i++;
			}
		}
#if DEBUG
		if (!cur->waiters_size) {
			dprint("DL DETECT[REMOVE] --- No dependencies left on node (%d,%d,%ld)\n",
					cur->srv, cur->fib, cur->ts);
		} else {
			for (int i = 0; i < cur->waiters_size; i ++) {
				dprint("DL DETECT[REMOVE] --- Now have dependency: (%d,%d,%ld) ---> (%d,%d,%ld)\n",
						cur->srv, cur->fib, cur->ts,
						cur->neighbors[i].srv, cur->neighbors[i].fib, cur->neighbors[i].ts);
			}
		}
#endif
	}
}

void mp_dl_detect_clear_dependencies(struct dl_detect_graph_node *src) {
	int idx = get_node_index(src);
	assert(idx != -1);
	struct dl_detect_graph_node *cur = &graph_nodes[idx];
	cur->waiters_size = 0;
	cur->has_aborted = 1;
}

