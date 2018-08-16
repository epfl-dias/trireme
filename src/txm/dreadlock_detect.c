/*
 * dreadlock_detect.c
 *
 *  Created on: Jul 19, 2017
 *      Author: anadioti
 */

#include "dreadlock_detect.h"
#include "headers.h"

void dreadlock_init() {
	printf("Initializing dreadlock-detect\n");
	int nof_servers = g_nservers * g_nfibers;

	all_deps = (dreadlock_deps **) malloc(nof_servers * sizeof(dreadlock_deps *));
	tid_graph = (uint64_t **) malloc(nof_servers * sizeof(uint64_t *));
	backup_tid_graph = (uint64_t **) calloc(nof_servers, sizeof(uint64_t *));
	active_ts = (uint64_t *) malloc(nof_servers * sizeof(uint64_t));
	deps_per_server = (uint32_t *) malloc(nof_servers * sizeof(uint32_t));
	dep_add_mtx = (pthread_mutex_t *) malloc(nof_servers * sizeof(pthread_mutex_t));
	spin = (volatile int *) malloc(nof_servers * sizeof(int));
	updated_positions = (uint16_t **) malloc(nof_servers * sizeof(uint16_t *));

	for (int i = 0; i < nof_servers; i++) {
		all_deps[i] = (dreadlock_deps *) malloc(nof_servers * sizeof(dreadlock_deps));
		tid_graph[i] = (uint64_t *) malloc(nof_servers * sizeof(uint64_t));
		updated_positions[i] = (uint16_t *) malloc(nof_servers * sizeof(uint16_t));
		backup_tid_graph[i] = (uint64_t *) malloc(nof_servers * sizeof(uint64_t));
		pthread_mutex_init(&dep_add_mtx[i], NULL);

		memset(&all_deps[i][0], 0, nof_servers * sizeof(dreadlock_deps));
		memset(&tid_graph[i][0], 0, nof_servers * sizeof(uint64_t));
		memset(&backup_tid_graph[i][0], 0, nof_servers * sizeof(uint64_t));
		memset(&updated_positions[i][0], 0, nof_servers * sizeof(uint16_t));
	}

	memset(active_ts, 0, nof_servers * sizeof(uint64_t));
	memset(deps_per_server, 0, nof_servers * sizeof(uint32_t));
}

void dreadlock_add(int cur_srv, uint64_t cur_ts, int *trg_srv, uint64_t *trg_tid, int deps) {
	pthread_mutex_lock(&dep_add_mtx[cur_srv]);
	int nof_servers = g_nservers * g_nfibers;
	if (cur_ts != active_ts[cur_srv]) {
		deps_per_server[cur_srv] = 0;
	}
	active_ts[cur_srv] = cur_ts;
	int cur_srv_deps = deps_per_server[cur_srv];
	for (int i = 0; i < deps; i++) {
		all_deps[cur_srv][cur_srv_deps].trg_srv = trg_srv[i];
		all_deps[cur_srv][cur_srv_deps].trg_tid = trg_tid[i];
		cur_srv_deps ++;
	}
	deps_per_server[cur_srv] = cur_srv_deps;
	pthread_mutex_unlock(&dep_add_mtx[cur_srv]);
}

int dreadlock_wait(int cur_srv, uint64_t cur_tid, volatile char *ready) {
	int nof_servers = g_nservers * g_nfibers;

	// first update the local dependencies
	int cur_srv_deps = deps_per_server[cur_srv];
	for (int i = 0; i < cur_srv_deps; i++) {
		tid_graph[cur_srv][all_deps[cur_srv][i].trg_srv] = all_deps[cur_srv][i].trg_tid;
	}

	// now spin until a deadlock is found or a notification arrives
	while(!(*ready)) {
		// get the remote dependencies only if the local dependency is still valid
		for (int i = 0; (i < cur_srv_deps) && (!(*ready)); i++) {
			int dep_srv = all_deps[cur_srv][i].trg_srv;

			if (all_deps[cur_srv][i].trg_tid == active_ts[dep_srv]) {
				int updates = 0;
				for (int j = 0; j < nof_servers; j++) {
					if (tid_graph[cur_srv][j] < tid_graph[dep_srv][j]) {
						updated_positions[cur_srv][updates++] = j;
						backup_tid_graph[cur_srv][j] = tid_graph[dep_srv][j];
						assert(dep_srv == all_deps[cur_srv][i].trg_srv);
					}
				}
				if ((updates) && (all_deps[cur_srv][i].trg_tid == active_ts[dep_srv])) {
					for (int j = 0; j < updates; j ++) {
						tid_graph[cur_srv][updated_positions[cur_srv][j]] = backup_tid_graph[cur_srv][updated_positions[cur_srv][j]];
					}
				}
				if (tid_graph[cur_srv][cur_srv] == cur_tid) {
					// we have a deadlock
					spin[cur_srv] = 0;
					deps_per_server[cur_srv] = 0;
					return 1;
				}
			}
		}
		if (cur_srv_deps < deps_per_server[cur_srv]) {
			int upd_deps_per_server = deps_per_server[cur_srv];
			for (int i = cur_srv_deps; i < upd_deps_per_server; i++) {
				tid_graph[cur_srv][all_deps[cur_srv][i].trg_srv] = all_deps[cur_srv][i].trg_tid;
			}
			cur_srv_deps = upd_deps_per_server;
		}
	}

	deps_per_server[cur_srv] = 0;
	return 0;
}

void dreadlock_notify(int srv) {
	spin[srv] = 0;
}
