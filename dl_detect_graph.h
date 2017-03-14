/*
 * dl_detect_graph.h
 *
 *  Created on: Mar 2, 2017
 *      Author: anadioti
 */

#ifndef DL_DETECT_GRAPH_H_
#define DL_DETECT_GRAPH_H_

#include <stdlib.h>
#include <stdint.h>
#include <sys/queue.h>

#define NODES_INDEX_SIZE 1//997

LIST_HEAD(waiting_elements_list, waiting_elements_entry);
struct waiting_elements_entry {
	struct elem *e;
	int sender;
	LIST_ENTRY(waiting_elements_entry) nxt_waiting_elem;
};

struct node {
	int srv;
	int fib;
	uint64_t ts;
	int sender_srv;
	uint64_t added_ts;
	uint64_t elem_addr_to_add;
	struct waiting_elements_list wel;
	int visited;
	int neighbors;

	struct node *linked_node;
};

//TAILQ_HEAD(node_q_head, node_q_entry);
//struct node_q_entry {
//	struct node txn_node;
//	TAILQ_ENTRY(node_q_entry) node_q_entries;
//};

LIST_HEAD(node_q_head, node_q_entry);
struct node_q_entry {
	struct node txn_node;
	LIST_ENTRY(node_q_entry) node_q_entries;
};

LIST_HEAD(hash_node_list, hash_node_list_entry);
struct hash_node_list_entry {
	struct node_q_head *nodes;
	LIST_ENTRY(hash_node_list_entry) next_hash_node_list_entry;
};

struct hash_node_list *nodes_index;

void dl_detect_graph_init();
void dl_detect_graph_add_dependency(struct node src_node, struct node *trg_nodes, int nof_targets);
int dl_detect_graph_detect_cycle(struct node src_node, struct node *deadlock_node);
void dl_detect_graph_rmv_dependency(struct node *src, struct node *trg);
void dl_detect_graph_clear_dependency(struct node to_remove);


#endif /* DL_DETECT_GRAPH_H_ */
