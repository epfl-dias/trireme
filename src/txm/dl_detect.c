//#if ENABLE_DL_DETECT_CC
#include <sys/queue.h>
#include <stdio.h>
#include <stdlib.h>

#include "headers.h"
#include "dl_detect.h"
#include "assert.h"
#include "plmalloc.h"
#include "util.h"
#include "glo.h"

typedef int64_t SInt64;
typedef int32_t SInt32;

// TODO: FIXME
int get_thdid_from_txnid(uint64_t txnid) {
	return txnid;
}

/********************************************************/
// The current txn aborts itself only if it holds less
// locks than all the other txns on the loop. 
// In other words, the victim should be the txn that 
// performs the least amount of work
/********************************************************/
void DL_detect_init(DL_detect *ptr) {
	dprint("Initializing dl detect\n");
	ptr->dependency = (DepThd *) malloc(g_nservers * g_nfibers * sizeof(DepThd));
	ptr->V = g_nservers * g_nfibers;
	for (int i = 0; i < g_nservers * g_nfibers; i ++) {
		pthread_mutex_init(&ptr->dependency[i].lock, NULL);
		LIST_INIT(&ptr->dependency[i].adj);
		ptr->dependency[i].adj_size = 0;
		ptr->dependency[i].txnid = i;
	}
}

int
DL_detect_add_dep_ts(struct partition *p, DL_detect *ptr, int srv_fib_id,
        uint64_t txnid1, uint64_t * txnids, int cnt, int num_locks) {

	int thd1 = get_thdid_from_txnid(srv_fib_id);
	pthread_mutex_lock( &ptr->dependency[thd1].lock );
	ptr->dependency[thd1].txnid = txnid1;
	ptr->dependency[thd1].num_locks = num_locks;

	for (int i = 0; i < cnt; i++)  {
		struct adj_list_entry *list_entry =
            (struct adj_list_entry *) plmalloc_alloc(p, sizeof(struct adj_list_entry));

		list_entry->entry = txnids[i];
		LIST_INSERT_HEAD(&ptr->dependency[thd1].adj, list_entry, next);
		ptr->dependency[thd1].adj_size ++;
		dprint("--> Added dependency %d to thread %d\n", txnids[i], thd1);

	}
	dprint("List size of thd %d now is %d\n", thd1, ptr->dependency[thd1].adj_size);
	pthread_mutex_unlock( &ptr->dependency[thd1].lock );
	return 0;
}

int
DL_detect_add_dep(struct partition *p, DL_detect *ptr, uint64_t txnid1,
        uint64_t * txnids, int cnt, int num_locks) {

	int thd1 = get_thdid_from_txnid(txnid1);
	pthread_mutex_lock( &ptr->dependency[thd1].lock );
	ptr->dependency[thd1].txnid = txnid1;
	ptr->dependency[thd1].num_locks = num_locks;
	
	for (int i = 0; i < cnt; i++)  {
		struct adj_list_entry *list_entry =
            (struct adj_list_entry *) plmalloc_alloc(p, sizeof(struct adj_list_entry));

		list_entry->entry = txnids[i];
		LIST_INSERT_HEAD(&ptr->dependency[thd1].adj, list_entry, next);
		ptr->dependency[thd1].adj_size ++;
		dprint("--> Added dependency %d to thread %d\n", txnids[i], thd1);

	}

	dprint("List size of thd %d now is %d\n", thd1, ptr->dependency[thd1].adj_size);
	pthread_mutex_unlock( &ptr->dependency[thd1].lock );
	return 0;
}

int
DL_detect_remove_dep(DL_detect *ptr, uint64_t to_remove) {
	int ret = 0;
	for (int thd = 0; thd < ptr->V; thd ++) {
		pthread_mutex_lock( &ptr->dependency[thd].lock );
		struct adj_list_entry *cur = LIST_FIRST(&ptr->dependency[thd].adj);
		while (cur != NULL) {
			if (cur->entry == to_remove) {
				LIST_REMOVE(cur, next);
				ptr->dependency[thd].adj_size--;
				dprint("[DL_DETECT] Removed txn %d from the list of %d\n", to_remove, thd);
				dprint("[DL_DETECT] Now thd %d has %d dependencies \n", thd, ptr->dependency[thd].adj_size);
			}
			cur = LIST_NEXT(cur, next);
		}
		pthread_mutex_unlock( &ptr->dependency[thd].lock );
	}

	return ret;
}

bool 
DL_detect_nextNode(DL_detect *ptr, uint64_t txnid, DetectData * detect_data) {
	int thd = get_thdid_from_txnid(txnid);
	assert( !detect_data->visited[thd] );
	detect_data->visited[thd] = true;
	detect_data->recStack[thd] = true;
	pthread_mutex_lock( &ptr->dependency[thd].lock );
	int lock_num = ptr->dependency[thd].num_locks;
	int txnid_num = ptr->dependency[thd].adj_size;
	dprint("Thread %d has %d dependencies\n", thd, txnid_num);
	uint64_t txnids[ txnid_num ];
	int n = 0;
	
	if (ptr->dependency[thd].txnid != (SInt64)txnid) {
		detect_data->recStack[thd] = false;
		pthread_mutex_unlock( &ptr->dependency[thd].lock );
		return false;
	}
	
	struct adj_list_entry *entry_it;
	int cnt = 0;
	LIST_FOREACH(entry_it, &ptr->dependency[thd].adj, next) {
		txnids[n++] = entry_it->entry;
		dprint("Added element %d\n", entry_it->entry);
	}
	
	pthread_mutex_unlock( &ptr->dependency[thd].lock );

	for (n = 0; n < txnid_num; n++) {
		int nextthd = get_thdid_from_txnid( txnids[n] );

		// next node not visited and txnid is not stale
		if ( detect_data->recStack[nextthd] ) {
			if ((SInt32)txnids[n] == ptr->dependency[nextthd].txnid) {
				detect_data->loop = true;
				detect_data->onloop = true;
				detect_data->loopstart = nextthd;
				break;
			}
		} 
		if ( !detect_data->visited[nextthd] && 
				ptr->dependency[nextthd].txnid == (SInt64) txnids[n] &&
				DL_detect_nextNode(ptr, txnids[n], detect_data))
		{
			break;
		}
	}

	detect_data->recStack[thd] = false;
	if (detect_data->loop 
			&& detect_data->onloop 
			&& lock_num < detect_data->min_lock_num) {
		detect_data->min_lock_num = lock_num;
		detect_data->min_txnid = txnid;
	}
	if (thd == detect_data->loopstart) {
		detect_data->onloop = false;
	}
	return detect_data->loop;
}

// isCycle returns true if there is a loop AND the current txn holds the least 
// number of locks on that loop.
bool DL_detect_isCyclic(DL_detect *ptr, uint64_t txnid, DetectData * detect_data) {
	return DL_detect_nextNode(ptr, txnid, detect_data);
}

int
DL_detect_detect_cycle(struct partition *p, DL_detect *ptr, uint64_t txnid) {
	uint64_t starttime = get_sys_clock();
	bool deadlock = false;

	int thd = get_thdid_from_txnid(txnid);

	DetectData * detect_data = (DetectData *) plmalloc_alloc(p, sizeof(DetectData));
	detect_data->visited = (bool *) plmalloc_alloc(p, sizeof(bool) * ptr->V);
	detect_data->recStack = (bool *) plmalloc_alloc(p, sizeof(bool) * ptr->V);

	for(int i = 0; i < ptr->V; i++) {
        detect_data->visited[i] = false;
		detect_data->recStack[i] = false;
	}

	detect_data->min_lock_num = 1000;
	detect_data->min_txnid = -1;
	detect_data->loop = false;

	if ( DL_detect_isCyclic(ptr, txnid, detect_data) ){
		deadlock = true;
		int thd_to_abort = get_thdid_from_txnid(detect_data->min_txnid);
		if (ptr->dependency[thd_to_abort].txnid == (SInt64) detect_data->min_txnid) {
			// TODO: Handle abort here?
		}
	} 
	
	plmalloc_free(p, detect_data->visited, sizeof(bool) * ptr->V);
	plmalloc_free(p, detect_data->recStack, sizeof(bool) * ptr->V);
	plmalloc_free(p, detect_data, sizeof(DetectData));

	uint64_t timespan = get_sys_clock() - starttime;
	if (deadlock) return 1;
	else return 0;
}

void DL_detect_clear_dep(struct partition *p, DL_detect *ptr, uint64_t txnid) {
	int thd = get_thdid_from_txnid(txnid);
	pthread_mutex_lock( &ptr->dependency[thd].lock );
	dprint("Clearing dependency for thread %d with txnid = %d\n", thd, txnid);
	if (ptr == NULL) {
		dprint("NULL pointer\n");
	}

	while (LIST_FIRST(&ptr->dependency[thd].adj) != NULL) {
		struct adj_list_entry *first_entry = LIST_FIRST(&ptr->dependency[thd].adj);
		LIST_REMOVE(first_entry, next);
		plmalloc_free(p, first_entry, sizeof(struct adj_list_entry));
	}
	ptr->dependency[thd].adj_size = 0;
	ptr->dependency[thd].num_locks = 0;
	pthread_mutex_unlock( &ptr->dependency[thd].lock );
}
//#endif
