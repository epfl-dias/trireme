
/*
 * master.c
 *
 *  Created on: Nov 28, 2017
 *      Author: anadioti
 */

#include "headers.h"
#include "benchmark.h"
#include "worker.h"
#include "smphashtable.h"
#include "se_dl_detect_graph.h"
#include "partition.h"

void *hash_table_server(void* args)
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

	double tstart = now();

#if SHARED_EVERYTHING
	/* load only one partition in case of shared everything */
	//if (s == 0)
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

	pthread_mutex_lock(&hash_table->create_client_lock);

	nready++;

	pthread_mutex_unlock(&hash_table->create_client_lock);

	while (nready != g_active_servers) ;

	printf("srv %d starting txns\n", s);
	fflush(stdout);

	query = g_benchmark->alloc_query();
	assert(query);

	tstart = now();

#if ENABLE_ASYMMETRIC_MESSAGING

#if defined(SHARED_EVERYTHING) || defined(SHARED_NOTHING)
#error "Asymmetric messaging valid only in msgpassing mode\n"
#endif

	if (s >= g_nhot_servers)
		task_libinit(s);
#else
	task_libinit(s);
#endif

	tend = now();

	printf("srv %d query time %.3f\n", s, tend - tstart);
	printf("srv %d total txns %lu \n", s, p->q_idx);
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

	return NULL;
}

void start_hash_table_servers_hotplug(struct hash_table *hash_table, int hotplugged_servers)
{
	int r;
	void *value;
	nready = 0;

	int old_g_active_servers = g_active_servers;
	g_active_servers += hotplugged_servers;
	assert(g_active_servers <= g_nservers);
	assert(g_active_servers <= NCORES);


	for (int i = old_g_active_servers; i < g_active_servers; i++) {
		hash_table->thread_data[i].id = i;
		hash_table->thread_data[i].core = i;
		hash_table->thread_data[i].hash_table = hash_table;
		printf("Assinging core %d to srv %d\n", hash_table->thread_data[i].core, i);

		r = pthread_create(&hash_table->threads[i], NULL, hash_table_server, (void *) (&hash_table->thread_data[i]));
		assert(r == 0);
	}

	for (int i = 0; i < g_active_servers; i++) {
		r = pthread_join(hash_table->threads[i], &value);
		assert(r == 0);
	}
}


void *hotplug_master(void *args)
{
	struct hash_table *hash_table = (struct hash_table *) args;

	set_affinity(0);

	// periodically start a bunch of servers
	int cnt = g_active_servers;
	while (cnt < g_nservers) {
		usleep(HOTPLUG_WAIT_TIME);
		printf("Total tps until this point: %0.9fM\n", stats_get_tps(hash_table));
		start_worker_hotplug(cnt, hash_table);
		notify_worker(cnt++, WORKER_ACTION_START_TXNS, hash_table);
	}

	return NULL;
}

void start_hash_table_servers(struct hash_table *hash_table)
{
#if ENABLE_DL_DETECT_CC
	se_dl_detect_init_dependency_graph();
#if !defined(SHARED_EVERYTHING)
#include "twopl.h"
	dl_detect_init_data_structures();
#endif
#endif

	int r;
	void *value;
	nready = hash_table->quitting = 0;
	pthread_t cur_thread;

	assert(NCORES >= g_active_servers);

	printf("Active servers = %d\n", g_active_servers);
	for (int i = 0; i < g_active_servers; i++) {
//		hash_table->thread_data[i].id = i;
//#if ENABLE_VIRTUALIZATION
//		hash_table->thread_data[i].core = i;
//#else
//		hash_table->thread_data[i].core = coreids[i];
//#endif
//		hash_table->thread_data[i].hash_table = hash_table;
//
//		printf("Assinging core %d to srv %d\n", hash_table->thread_data[i].core, i);


		start_worker(i, hash_table);

		// worker load data
		notify_worker(i, WORKER_ACTION_LOAD_DATA, hash_table);
		// worker start transactions
		notify_worker(i, WORKER_ACTION_START_TXNS, hash_table);

//		r = pthread_create(&hash_table->threads[i], NULL, hash_table_server, (void *) (&hash_table->thread_data[i]));
//		assert(r == 0);
	}

	/* wait for everybody to start */
	while (nready != g_active_servers) ;

	r = pthread_create(&cur_thread, NULL, hotplug_master, (void *) hash_table);
	assert(r == 0);

	/* sleep for preconfigured time */
	usleep(RUN_TIME);

	hash_table->quitting = 1;

	pthread_join(cur_thread, NULL);

	for (int i = 0; i < g_active_servers; i++) {
//		notify_worker(hash_table->thread_data[i].id, WORKER_ACTION_STOP_TXNS, hash_table);
		stop_worker(i, hash_table);
//		r = pthread_join(hash_table->threads[i], &value);
//		assert(r == 0);
	}
}
