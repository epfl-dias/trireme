/*
 * se_dl_detect_graph.h
 *
 *  Created on: Mar 20, 2017
 *      Author: anadioti
 */

#ifndef SE_DL_DETECT_GRAPH_H_
#define SE_DL_DETECT_GRAPH_H_

#include <stdint.h>
#include <pthread.h>
#include <string.h>

#include "type.h"
#include "glo.h"

struct se_waiter_node {
	uint64_t srvfib;
	uint64_t ts;
	int opid;
};

struct se_dl_detect_graph_node {
	uint64_t srvfib;
	uint64_t ts;
	int opid;
	struct elem *e;
	int sender_srv;

	struct se_waiter_node *neighbors;
	unsigned int waiters_size;

	short visited;
	short has_aborted;
};

#define NODE_DEEP_COPY(src, dst) {								\
	memcpy((dst), (src), sizeof(struct se_dl_detect_graph_node));		\
	memcpy((dst)->neighbors, (src)->neighbors, 						\
			(src)->waiters_size * sizeof(struct se_waiter_node));	\
}

#define NODE_SHALLOW_COPY(src, dst) {								\
	memcpy((dst), (src), sizeof(struct se_dl_detect_graph_node));		\
}

struct se_dl_detect_graph_node *se_graph_nodes;

struct se_dl_detect_graph_node **se_graph_nodes_copy;
int **se_cycle_remaining_nodes;
uint64_t **se_cycle_remaining_nodes_ts;
int **se_cycle_visited_nodes;
int **se_cycle_added_nodes;

void se_dl_detect_init_dependency_graph();
int se_dl_detect_add_dependency(struct se_dl_detect_graph_node *src);
int se_dl_detect_detect_cycle(int thd, struct se_dl_detect_graph_node *src);
void se_dl_detect_remove_dependency(struct se_dl_detect_graph_node *src);
void se_dl_detect_clear_dependencies(struct se_dl_detect_graph_node *src, int is_abort);

pthread_mutex_t *mtx;

#endif /* SE_DL_DETECT_GRAPH_H_ */
