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

void start_worker(int id, struct hash_table *hash_table);
void start_worker_hotplug(int id, struct hash_table *hash_table);
void stop_worker(int id, struct hash_table *hash_table);
void notify_worker(int id, int action, struct hash_table *hash_table);

#endif /* WORKER_H_ */

