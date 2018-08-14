/*
 * se_dl_detect_graph.c
 *
 *  Created on: Mar 20, 2017
 *      Author: anadioti
 */

#include <stdio.h>
#include <string.h>
#include <assert.h>
#include <stdlib.h>

#include "se_dl_detect_graph.h"
#include "util.h"

int all_servers_cnt;

int se_nodes_equal(struct se_waiter_node *node1, struct se_waiter_node *node2) {
	int ret = 0;

	if ((node1->srvfib == node2->srvfib) &&
			(node1->ts == node2->ts) &&
			(node1->opid == node2->opid)) {
		ret = 1;
	}

	return ret;
}

void se_dl_detect_init_dependency_graph() {
	all_servers_cnt = g_nservers * g_nfibers;

	se_cycle_remaining_nodes = (int **) malloc(all_servers_cnt * sizeof(int *));
	se_cycle_remaining_nodes_ts = (uint64_t **) malloc(all_servers_cnt * sizeof(uint64_t *));
	se_cycle_visited_nodes = (int **) malloc(all_servers_cnt * sizeof(int *));
	se_cycle_added_nodes = (int **) malloc(all_servers_cnt * sizeof(int *));
	se_graph_nodes_copy = (struct se_dl_detect_graph_node **) malloc(all_servers_cnt * sizeof(struct se_dl_detect_graph_node *));

	se_graph_nodes = (struct se_dl_detect_graph_node *) malloc(all_servers_cnt * sizeof(struct se_dl_detect_graph_node));
	mtx = (pthread_mutex_t *) malloc(all_servers_cnt * sizeof(pthread_mutex_t));

	for (int i = 0; i < all_servers_cnt; i ++) {
		pthread_mutex_init(&mtx[i], NULL);

		se_graph_nodes[i].ts = 0;
		se_graph_nodes[i].opid = 0;
		se_graph_nodes[i].e = NULL;
		se_graph_nodes[i].has_aborted = 0;
		se_graph_nodes[i].sender_srv = -1;
		se_graph_nodes[i].waiters_size = 0;
		se_graph_nodes[i].srvfib = UINTMAX_MAX;
		se_graph_nodes[i].neighbors = (struct se_waiter_node *) malloc(all_servers_cnt * sizeof(struct se_waiter_node));

		se_graph_nodes_copy[i] = (struct se_dl_detect_graph_node *) malloc(all_servers_cnt * sizeof(struct se_dl_detect_graph_node));
		for (int j = 0; j < all_servers_cnt; j ++) {
			se_graph_nodes_copy[i][j].neighbors = (struct se_waiter_node *) calloc(all_servers_cnt, sizeof(struct se_waiter_node));
			dprint("Allocating %d position for neighbors of node %d thd %d\n", all_servers_cnt, j, i);
		}
		se_cycle_remaining_nodes[i] = (int *) malloc(all_servers_cnt * sizeof(int));
		se_cycle_remaining_nodes_ts[i] = (uint64_t *) malloc(all_servers_cnt * sizeof(uint64_t));
		se_cycle_added_nodes[i] = (int *) malloc(all_servers_cnt * sizeof(int));
		se_cycle_visited_nodes[i] = (int *) malloc(all_servers_cnt * sizeof(int));
	}
	dprint("DL DETECT --- Initialized\n");
}

int se_dl_detect_add_dependency(struct se_dl_detect_graph_node *src) {
	uint64_t idx = src->srvfib;
	pthread_mutex_lock(&mtx[idx]);
	struct se_dl_detect_graph_node *cur = &se_graph_nodes[idx];
	if (cur->ts > src->ts) {	// out-of-order arrival
		dprint("DL DETECT[ADD] --- Not adding: Found node with ts %ld - req node ts %ld\n", cur->ts, src->ts);
		pthread_mutex_unlock(&mtx[idx]);
		return 0;
	} else if (cur->ts == src->ts) {	// arrival for an active txn
		if (cur->has_aborted) {
			pthread_mutex_unlock(&mtx[idx]);
			return 0;
		}
		if (cur->opid > src->opid) {	// arrival for an old operation
			dprint("DL DETECT[ADD] --- Not adding: Found node with opid %d - req node opid %d\n", cur->opid, src->opid);
			pthread_mutex_unlock(&mtx[idx]);
			return 0;
		} else if (cur->opid == src->opid) {	// arrival for the current operation
			// add the targets
			memcpy(&cur->neighbors[cur->waiters_size], &src->neighbors[0], src->waiters_size * sizeof(struct se_waiter_node));
			cur->waiters_size += src->waiters_size;
			cur->e = src->e;
			cur->sender_srv = src->sender_srv;
		} else {	// arrival for a new operation
			// we have a new request; the old targets can be replaced
			cur->waiters_size = 0;
			// add the targets
			memcpy(&cur->neighbors[cur->waiters_size], &src->neighbors[0], src->waiters_size * sizeof(struct se_waiter_node));
			cur->waiters_size = src->waiters_size;
			// finally set the new element the txn is waiting for and the server that sent the new dependency
			cur->e = src->e;
			cur->sender_srv = src->sender_srv;
			cur->opid = src->opid;
		}
	} else {	// new txn arrival
		cur->e = src->e;
		cur->srvfib = src->srvfib;
		cur->opid = src->opid;
		cur->sender_srv = src->sender_srv;
		cur->ts = src->ts;
		cur->visited = src->visited;
		cur->waiters_size = src->waiters_size;
		cur->has_aborted = 0;

		memcpy(cur->neighbors, src->neighbors, src->waiters_size * sizeof(struct se_waiter_node));
	}

#if DEBUG
	dprint("DL DETECT[ADD] --- Added deps from node (%"PRIu64",%ld) after a request by server %d key %ld\n", src->srvfib, src->ts, src->sender_srv, src->e->key);
	for (int i = 0; i < cur->waiters_size; i ++) {
		dprint("DL DETECT[ADD] --- Now have dependency: (%"PRIu64",%ld) ---> (%"PRIu64",%ld)\n",
				cur->srvfib, cur->ts,
				cur->neighbors[i].srvfib, cur->neighbors[i].ts);
	}
#endif
	pthread_mutex_unlock(&mtx[idx]);

	return 1;
}

int se_iterative_cycle_check(int thd, struct se_dl_detect_graph_node *src) {
	int ret = 0;

	int src_idx = src->srvfib;
	int idx = src->srvfib;
	uint64_t ts = src->ts;

	int cycle_remaining_nodes_w_idx = 0;
	int cycle_remaining_nodes_r_idx = 0;
	memset(se_cycle_visited_nodes[thd], 0, all_servers_cnt * sizeof(int));
	memset(se_cycle_added_nodes[thd], 0, all_servers_cnt * sizeof(int));
	se_cycle_remaining_nodes[thd][cycle_remaining_nodes_w_idx] = idx;
	se_cycle_remaining_nodes_ts[thd][cycle_remaining_nodes_w_idx++] = ts;

	while (cycle_remaining_nodes_r_idx < cycle_remaining_nodes_w_idx) {
		idx = se_cycle_remaining_nodes[thd][cycle_remaining_nodes_r_idx];
		ts = se_cycle_remaining_nodes_ts[thd][cycle_remaining_nodes_r_idx++];

		dprint("DL DETECT[CYCLE-(%"PRIu64",%ld)] --- Checking node (%d,%ld)\n", src->srvfib, src->ts, idx, ts);
		if (!se_cycle_visited_nodes[thd][idx]) {
			se_cycle_visited_nodes[thd][idx] = 1;

			pthread_mutex_lock(&mtx[idx]);
//			memcpy(&se_graph_nodes_copy[thd][idx], &se_graph_nodes[idx], sizeof(struct se_dl_detect_graph_node));
			if (se_graph_nodes[idx].ts == ts) {
				se_graph_nodes_copy[thd][idx].waiters_size = se_graph_nodes[idx].waiters_size;
				se_graph_nodes_copy[thd][idx].srvfib = se_graph_nodes[idx].srvfib;
				se_graph_nodes_copy[thd][idx].ts = se_graph_nodes[idx].ts;
				se_graph_nodes_copy[thd][idx].opid = se_graph_nodes[idx].opid;

				if (se_graph_nodes[idx].waiters_size) {
					memcpy(se_graph_nodes_copy[thd][idx].neighbors, se_graph_nodes[idx].neighbors,
							se_graph_nodes[idx].waiters_size * sizeof(struct se_waiter_node));
				}

				pthread_mutex_unlock(&mtx[idx]);

				for (int i = 0; i < se_graph_nodes_copy[thd][idx].waiters_size; i ++) {
					assert(se_graph_nodes_copy[thd][idx].neighbors[i].ts > 0);

					int waiter_index = se_graph_nodes_copy[thd][idx].neighbors[i].srvfib;
					assert(waiter_index < all_servers_cnt);

					pthread_mutex_lock(&mtx[waiter_index]);
	//				memcpy(&se_graph_nodes_copy[thd][waiter_index], &se_graph_nodes[waiter_index], sizeof(struct se_dl_detect_graph_node));

					if (se_graph_nodes[waiter_index].ts) {
						se_graph_nodes_copy[thd][waiter_index].waiters_size = se_graph_nodes[waiter_index].waiters_size;
						se_graph_nodes_copy[thd][waiter_index].srvfib = se_graph_nodes[waiter_index].srvfib;
						se_graph_nodes_copy[thd][waiter_index].ts = se_graph_nodes[waiter_index].ts;

						pthread_mutex_unlock(&mtx[waiter_index]);
						dprint("DL DETECT[CYCLE-(%"PRIu64",%ld)] --- Trying waiter (%ld,%ld)\n", src->srvfib, src->ts,
								se_graph_nodes_copy[thd][waiter_index].srvfib,
								se_graph_nodes_copy[thd][waiter_index].ts);
						if (se_graph_nodes_copy[thd][waiter_index].ts == se_graph_nodes_copy[thd][idx].neighbors[i].ts) {
							dprint("DL DETECT[CYCLE-(%"PRIu64",%ld)] --- Waiter found\n", src->srvfib, src->ts)
							if (waiter_index == src_idx) {
								return 1;
							}
							if (!se_cycle_added_nodes[thd][waiter_index]) {
								se_cycle_added_nodes[thd][waiter_index] = 1;
								se_cycle_remaining_nodes[thd][cycle_remaining_nodes_w_idx] = waiter_index;
								se_cycle_remaining_nodes_ts[thd][cycle_remaining_nodes_w_idx++] = se_graph_nodes_copy[thd][waiter_index].ts;
							}
						}
					} else {
						pthread_mutex_unlock(&mtx[waiter_index]);
					}
				}
			} else {
				pthread_mutex_unlock(&mtx[idx]);
			}

		}
		assert(cycle_remaining_nodes_r_idx <= all_servers_cnt);
		assert(cycle_remaining_nodes_w_idx <= all_servers_cnt);
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

int se_dl_detect_detect_cycle(int thd, struct se_dl_detect_graph_node *src) {
	int idx = src->srvfib;
	assert(se_graph_nodes[idx].ts == src->ts);
	int deadlock = 0;
	dprint("DL DETECT[CYCLE] --- Start cycle check for node (%"PRIu64",%ld) from thread %d\n", src->srvfib, src->ts, thd);
#if RECURSIVE_CYCLE_CHECK
	int ret = next_node(&graph_nodes[idx], deadlock_node, &deadlock);
#else
	deadlock = se_iterative_cycle_check(thd, &se_graph_nodes[idx]);
#endif

#if DEBUG
	if (!deadlock) {
		dprint("DL DETECT[CYCLE] --- No DEADLOCK\n");
	} else {
		dprint("DL DETECT[CYCLE] --- DEADLOCK NODE (%"PRIu64",%ld) KEY %ld\n",
					src->srvfib, src->ts, src->e->key);
	}
#endif
	se_graph_nodes[idx].visited = 0;

	return deadlock;
}

void se_dl_detect_remove_dependency(struct se_dl_detect_graph_node *src) {
	int idx = src->srvfib;
	pthread_mutex_lock(&mtx[idx]);

	struct se_dl_detect_graph_node *cur = &se_graph_nodes[idx];
	if (cur->ts == src->ts) {
		for (int j = 0; j < src->waiters_size; j ++) {
			int i = 0;
			while (i < cur->waiters_size) {
				if (se_nodes_equal(&cur->neighbors[i], &src->neighbors[j])) {
					memmove(&cur->neighbors[i], &cur->neighbors[i + 1], (cur->waiters_size - i - 1) * sizeof(struct se_waiter_node));
					cur->waiters_size--;
					break;
				}
				i++;
			}
		}
	}

	pthread_mutex_unlock(&mtx[idx]);
}

void se_dl_detect_clear_dependencies(struct se_dl_detect_graph_node *src, int is_abort) {
	int idx = src->srvfib;
	pthread_mutex_lock(&mtx[idx]);
	struct se_dl_detect_graph_node *cur = &se_graph_nodes[idx];
	if (cur->ts == src->ts) {
		cur->waiters_size = 0;
		cur->has_aborted = is_abort;
	}
	pthread_mutex_unlock(&mtx[idx]);
}




