/*
 * dl_detect.h
 *
 *  Created on: Jan 6, 2017
 *      Author: anadioti
 */
//#if ENABLE_DL_DETECT_CC
#ifndef DL_DETECT_H_
#define DL_DETECT_H_

#include <limits.h>
#include <sys/queue.h>
#include <stdbool.h>
#include <stdint.h>
#include "pthread.h"

#define DL_DETECT_TIMEOUT			1000000000 // 1ms
#define DL_LOOP_DETECT				100 	// 100 us
#define DL_LOOP_TRIAL				10	// 1 us
#define PAUSE usleep(1);

#define CL_SIZE	64

LIST_HEAD(adj_list, adj_list_entry);
struct adj_list_entry {
	uint64_t entry;
	LIST_ENTRY(adj_list_entry) next;
};

LIST_HEAD(int_pair_list, int_pair_list_entry);
struct int_pair_list_entry {
	uint64_t sender_srv;
	uint64_t cand_srv;
	uint64_t cand_fib;
	uint64_t cand_ts;
	struct elem *record_addr;
	int opid;
	LIST_ENTRY(int_pair_list_entry) next_int_pair;
};

// The denpendency information per thread
typedef struct {
	struct adj_list adj;
	size_t adj_size;
	pthread_mutex_t lock;
	volatile int64_t txnid; 				// -1 means invalid
	int num_locks;				// the # of locks that txn is currently holding
	char pad[2 * CL_SIZE - sizeof(int64_t) - sizeof(pthread_mutex_t) - sizeof(struct adj_list) - sizeof(int)];
} DepThd;

// shared data for a particular deadlock detection
typedef struct {
	bool * visited;
	bool * recStack;
	bool loop;
	bool onloop;		// the current node is on the loop
	int loopstart;		// the starting point of the loop
	int min_lock_num; 	// the min lock num for txn in the loop
	uint64_t min_txnid; // the txnid that holds the min lock num
} DetectData;

typedef struct {
	int V;    // No. of vertices
	DepThd * dependency;

	///////////////////////////////////////////
	// For deadlock detection
	///////////////////////////////////////////
	// dl_lock is the global lock. Only used when deadlock detection happens
	pthread_mutex_t _lock;
} DL_detect;

void DL_detect_init(DL_detect *ptr);
int DL_detect_add_dep_ts(DL_detect *ptr, int srv_fib_id, uint64_t txnid1, uint64_t * txnids, int cnt, int num_locks);
int DL_detect_add_dep(DL_detect *ptr, uint64_t txnid1, uint64_t * txnids, int cnt, int num_locks);
int DL_detect_remove_dep(DL_detect *ptr, uint64_t to_remove);
bool DL_detect_nextNode(DL_detect *ptr, uint64_t txnid, DetectData * detect_data);
bool DL_detect_isCyclic(DL_detect *ptr, uint64_t txnid, DetectData * detect_data);
int DL_detect_detect_cycle(DL_detect *ptr, uint64_t txnid);
void DL_detect_clear_dep(DL_detect *ptr, uint64_t txnid);

#endif /* DL_DETECT_H_ */
//#endif
