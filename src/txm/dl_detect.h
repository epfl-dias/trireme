/*
 * dl_detect.h
 *
 *  Created on: Jan 6, 2017
 *      Author: anadioti
 */
//#if ENABLE_DL_DETECT_CC
#ifndef DL_DETECT_H_
#define DL_DETECT_H_

#include "type.h"

#define DL_DETECT_TIMEOUT			1000000000 // 1ms
#define DL_LOOP_DETECT				100 	// 100 us
#define DL_LOOP_TRIAL				10	// 1 us
#define PAUSE usleep(1);

void DL_detect_init(DL_detect *ptr);
int DL_detect_add_dep_ts(struct partition *p, DL_detect *ptr, int srv_fib_id, uint64_t txnid1, uint64_t * txnids, int cnt, int num_locks);
int DL_detect_add_dep(struct partition *p, DL_detect *ptr, uint64_t txnid1, uint64_t * txnids, int cnt, int num_locks);
int DL_detect_remove_dep(DL_detect *ptr, uint64_t to_remove);
bool DL_detect_nextNode(DL_detect *ptr, uint64_t txnid, DetectData * detect_data);
bool DL_detect_isCyclic(DL_detect *ptr, uint64_t txnid, DetectData * detect_data);
int DL_detect_detect_cycle(struct partition *p, DL_detect *ptr, uint64_t txnid);
void DL_detect_clear_dep(struct partition *p, DL_detect *ptr, uint64_t txnid);

#endif /* DL_DETECT_H_ */
//#endif
