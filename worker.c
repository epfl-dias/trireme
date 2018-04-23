/*
 * worker.c
 *
 *  Created on: Nov 29, 2017
 *      Author: anadioti
 */

#include "headers.h"
#include "benchmark.h"
#include "worker.h"
#include "partition.h"
#include "smphashtable.h"
#if DIASSRV8

static int coreids[] = {
		1,2,3,4,5,6,7,8,9,10,
		11,12,13,14,15,16,17,18,19,20,
		21,22,23,24,25,26,27,28,29,30,
		31,32,33,34,35,36,37,38,39,40,
		0,81,82,83,84,85,86,87,88,89,
		90,91,92,93,94,95,96,97,98,99,
		100,101,102,103,104,105,106,107,108,109,
		110,111,112,113,114,115,116,117,118,119
};

#elif DIASCLD33
#if HT_ENABLED

static int coreids[] = {
		0,4,8,12,16,20,24,28,32,36,40,44,48,52,56,60,64,68,72,76,80,84,88,92,96,100,104,108,112,116,120,124,128,132,136,140,
		1,5,9,13,17,21,25,29,33,37,41,45,49,53,57,61,65,69,73,77,81,85,89,93,97,101,105,109,113,117,121,125,129,133,137,141,
		2,6,10,14,18,22,26,30,34,38,42,46,50,54,58,62,66,70,74,78,82,86,90,94,98,102,106,110,114,118,122,126,130,134,138,142,
		3,7,11,15,19,23,27,31,35,39,43,47,51,55,59,63,67,71,75,79,83,87,91,95,99,103,107,111,115,119,123,127,131,135,139,143,
};

#else

static int coreids[] = {
		0,4,8,12,16,20,24,28,32,36,40,44,48,52,56,60,64,68,
		1,5,9,13,17,21,25,29,33,37,41,45,49,53,57,61,65,69,
		2,6,10,14,18,22,26,30,34,38,42,46,50,54,58,62,66,70,
		3,7,11,15,19,23,27,31,35,39,43,47,51,55,59,63,67,71
};
#endif

#elif DIASCLD31
static int coreids[] = {
    0,2,4,6,8,10,12,14,16,18,20,22,
    1,3,5,7,9,11,13,15,17,19,21,23
};

#else
// for now, in default case, set it to 4 cores
static int coreids[] = {
    0,1,2,3
};

#endif

void load_data(struct hash_table *hash_table, int s)
{
	struct partition *p = &hash_table->partitions[s];
	double tstart = now();

#if SHARED_EVERYTHING
	/* load only one partition in case of shared everything */
	g_benchmark->load_data(hash_table, s);
#else

#if ENABLE_ASYMMETRIC_MESSAGING
	/* load only few partitions in case of asym msg. */
	if (s < g_nhot_servers)
		g_benchmark->load_data(hash_table, s);
#else
	/* always load for sn/trireme */
	g_benchmark->load_data(hash_table, s);

#endif //ENABLE_ASYMMETRIC_MESSAGING

#endif //SHARED_EVERYTHING

	double tend = now();

	printf("srv %d load time %.3f\n", s, tend - tstart);
	printf("srv %d rec count: %d partition sz %lu-KB "
			"tx count: %d, per_txn_op cnt: %d\n", s, p->ninserts, p->size / 1024,
			g_niters, g_ops_per_txn);

}

void execute_txns(struct hash_table *hash_table, int s)
{
	struct partition *p = &hash_table->partitions[s];
	void *query;

	pthread_mutex_lock(&hash_table->create_client_lock);

	nready++;

	pthread_mutex_unlock(&hash_table->create_client_lock);

	while (nready != g_active_servers) ;

	printf("srv %d starting txns\n", s);
	fflush(stdout);

	query = g_benchmark->alloc_query();
	assert(query);
	double tstart = now();

#if ENABLE_ASYMMETRIC_MESSAGING

#if defined(SHARED_EVERYTHING) || defined(SHARED_NOTHING)
#error "Asymmetric messaging valid only in msgpassing mode\n"
#endif

	if (s >= g_nhot_servers)
		task_libinit(s);
#else
	task_libinit(s);
#endif

	double tend = now();

	printf("srv %d query time %.3f\n", s, tend - tstart);
	printf("srv %d total txns %d \n", s, p->q_idx);
	printf("srv %d commited txns %d aborted %d\n", s, p->ncommits, p->naborts);

	fflush(stdout);

#if ENABLE_ASYMMETRIC_MESSAGING
	if (s < g_nhot_servers)
		p->tps = 0;
	else
		p->tps = p->q_idx / (tend - tstart);
#else
	p->tps = p->q_idx / (tend - tstart);
#endif

	pthread_mutex_lock(&hash_table->create_client_lock);

	nready--;

	pthread_mutex_unlock(&hash_table->create_client_lock);

	while (nready != 0)
#if !defined (SHARED_EVERYTHING) && !defined (SHARED_NOTHING)
		process_requests(hash_table, s);
#else
	;
#endif

	printf("srv %d quitting \n", s);
	fflush(stdout);

	if (g_benchmark->verify_txn)
		g_benchmark->verify_txn(hash_table, s);

	// destory out dses
#if !defined (SHARED_EVERYTHING)
	/* in shared nothing and trireme case, we can safely delete
	 * the partition now as no one else will be directly accessing
	 * data from the partition. In shared everything case, some other
	 * thread might still be doing verification. So we don't destroy now.
	 */
	destroy_hash_partition(p);
#endif
	printf("srv %d done quitting \n", s);
}

void *start_worker_thread(void *args)
{
	int i, r;
	const int s = ((struct thread_args *) args)->id;
	const int c = ((struct thread_args *) args)->core;
	struct hash_table *hash_table = ((struct thread_args *) args)->hash_table;
	struct partition *p = &hash_table->partitions[s];
	void *query;
	__attribute__((unused)) int pct = 10;

	set_affinity(c);
#if defined(MIGRATION)
	fix_affinity(s);
#endif

	printf("Worker %d waiting for signal\n", s);
	pthread_mutex_lock(&p->cv_mtx);
	while (p->signal_id == 0) {
		r = pthread_cond_wait(&p->cv, &p->cv_mtx);
	}
	p->actions[p->signal_id] = 1;
	pthread_mutex_unlock(&p->cv_mtx);
	printf("Worker %d received signal %d\n", s, p->signal_id);

	int loop_flag = 1;
	while (loop_flag) {
		for (int i = 0; i < WORKER_ACTIONS_NUMBER; i++) {
			pthread_mutex_lock(&p->cv_mtx);
			if (p->actions[i]) {
				switch(i) {
				case WORKER_ACTION_LOAD_DATA:
					printf("Worker %d loading data\n", s);
					p->actions[i] = 0;
					pthread_mutex_unlock(&p->cv_mtx);
					// load the data
					load_data(hash_table, s);
					break;
				case WORKER_ACTION_START_TXNS:
					printf("Worker %d executing txns\n", s);
					p->actions[i] = 0;
					pthread_mutex_unlock(&p->cv_mtx);
					execute_txns(hash_table, s);
					break;

				case WORKER_ACTION_STOP_TXNS:
					printf("Worker %d stopping\n", s);
					p->actions[i] = 0;
					pthread_mutex_unlock(&p->cv_mtx);
					loop_flag = 0;
					p->thread_termination_flag = 1;
					break;
				default:
					printf("Worker %d received msg %d\n", s, i);
				}
			} else {
				pthread_mutex_unlock(&p->cv_mtx);
			}
		}
	}

	return NULL;
}

void start_worker(int id, struct hash_table *hash_table)
{
	struct partition *p = &hash_table->partitions[id];
	hash_table->thread_data[id].id = id;
#if ENABLE_VIRTUALIZATION
	hash_table->thread_data[id].core = id;
#else
	hash_table->thread_data[id].core = coreids[id];
#endif
	hash_table->thread_data[id].hash_table = hash_table;

	printf("Assinging core %d to srv %d\n", hash_table->thread_data[id].core, id);
	pthread_mutex_init(&p->cv_mtx, NULL);
	p->signal_id = 0;
	p->action_id = 0;
	p->actions = (int *) calloc(WORKER_ACTIONS_NUMBER, sizeof(int));
	p->thread_termination_flag = 0;

	int r = pthread_create(&hash_table->threads[id], NULL, start_worker_thread, (void *) (&hash_table->thread_data[id]));
	assert(r == 0);
}

void start_worker_hotplug(int id, struct hash_table *hash_table)
{
	struct partition *p = &hash_table->partitions[id];
	hash_table->thread_data[id].id = id;
#if ENABLE_VIRTUALIZATION
	hash_table->thread_data[id].core = id;
#else
	hash_table->thread_data[id].core = coreids[id];
#endif
	hash_table->thread_data[id].hash_table = hash_table;

	printf("Assinging core %d to srv %d\n", hash_table->thread_data[id].core, id);
	pthread_mutex_init(&p->cv_mtx, NULL);
	p->signal_id = 0;
	p->action_id = 0;
	p->actions = (int *) calloc(WORKER_ACTIONS_NUMBER, sizeof(int));
	p->thread_termination_flag = 0;

	g_active_servers++;

	int r = pthread_create(&hash_table->threads[id], NULL, start_worker_thread, (void *) (&hash_table->thread_data[id]));
	assert(r == 0);
}


void stop_worker(int id, struct hash_table *hash_table)
{
	struct partition *p = &hash_table->partitions[id];
	pthread_mutex_lock(&p->cv_mtx);
	p->actions[WORKER_ACTION_STOP_TXNS] = 1;
	pthread_mutex_unlock(&p->cv_mtx);
	void *value;
	int r = pthread_join(hash_table->threads[id], &value);
	assert(r == 0);
}

void notify_worker(int id, int action, struct hash_table *hash_table)
{
	struct partition *p = &hash_table->partitions[id];
	pthread_mutex_lock(&p->cv_mtx);
	p->signal_id = action;
	p->actions[action] = 1;
	pthread_cond_signal(&p->cv);
	pthread_mutex_unlock(&p->cv_mtx);
	printf("Worker %d notified with signal %d\n", id, action);
}
