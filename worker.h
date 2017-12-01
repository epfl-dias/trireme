/*
 * worker.h
 *
 *  Created on: Dec 1, 2017
 *      Author: anadioti
 */

#ifndef WORKER_H_
#define WORKER_H_

#define WORKER_ACTIONS_NUMBER 3

#define WORKER_ACTION_LOAD_DATA 0
#define WORKER_ACTION_START_TXNS 1
#define WORKER_ACTION_STOP_TXNS 2

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

#else
// for now, in default case, set it to 4 cores
static int coreids[] = {
		0,1,2,3
};

#endif


void start_worker(int id, struct hash_table *hash_table);
void start_worker_hotplug(int id, struct hash_table *hash_table);
void stop_worker(int id, struct hash_table *hash_table);
void notify_worker(int id, int action, struct hash_table *hash_table);

#endif /* WORKER_H_ */
