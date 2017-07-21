/*
 * dreadlock_detect.h
 *
 *  Created on: Jul 19, 2017
 *      Author: anadioti
 */

#ifndef DREADLOCK_DETECT_H_
#define DREADLOCK_DETECT_H_

#include "headers.h"
#include "glo.h"

typedef struct {
	int trg_srv;
	uint64_t trg_tid;
} dreadlock_deps;

dreadlock_deps **all_deps;
uint32_t *deps_per_server;
pthread_mutex_t *dep_add_mtx;
uint64_t *active_ts;
uint16_t **updated_positions;

// each thread maintains an array with the tids that it depends on
uint64_t **tid_graph;
uint64_t **backup_tid_graph;
volatile int *spin;

void dreadlock_init();
void dreadlock_add(int cur_srv, uint64_t cur_ts, int *trg_srv, uint64_t *trg_tid, int deps);
int dreadlock_wait(int cur_srv, uint64_t cur_tid, volatile char *ready);
void dreadlock_notify(int srv);

#endif /* DREADLOCK_DETECT_H_ */
