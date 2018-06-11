/*
 * mp_dl_detect_graph.h
 *
 *  Created on: Mar 16, 2017
 *      Author: anadioti
 */

#ifndef MP_DL_DETECT_GRAPH_H_
#define MP_DL_DETECT_GRAPH_H_

#include <stdint.h>

#include "type.h"
#include "glo.h"

struct waiter_node {
	int srv;
	int fib;
	uint64_t ts;
	int opid;
};

struct dl_detect_graph_node {
	int srv;
	int fib;
	uint64_t ts;
	int opid;
	struct elem *e;
	int sender_srv;

	struct waiter_node *neighbors;
	unsigned int waiters_size;

	short visited;
	short has_aborted;
};

struct dl_detect_graph_node *graph_nodes;

int *cycle_visited_nodes;
int *cycle_remaining_nodes;
int *cycle_added_nodes;

void mp_dl_detect_init_dependency_graph();
int mp_dl_detect_add_dependency(struct dl_detect_graph_node *src);
int mp_dl_detect_detect_cycle(struct dl_detect_graph_node *src, struct dl_detect_graph_node *deadlock_node);
void mp_dl_detect_remove_dependency(struct dl_detect_graph_node *src);
void mp_dl_detect_clear_dependencies(struct dl_detect_graph_node *src);

#endif /* MP_DL_DETECT_GRAPH_H_ */
