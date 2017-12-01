/*
 * worker.c
 *
 *  Created on: Nov 29, 2017
 *      Author: anadioti
 */

#include "headers.h"
#include "benchmark.h"
#include "worker.h"

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


