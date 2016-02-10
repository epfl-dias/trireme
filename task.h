#ifndef __TASK_H__
#define __TASK_H__

//NTASKS depends on HASHOP_TID in const.h
#define NTASKS 15 

#define TASK_STACK_SIZE 1048576
#define ROOT_TASK_ID 0
#define UNBLOCK_TASK_ID 1
#define FIRST_TASK_ID 2

void task_libinit(int s);
void task_yield(struct partition *p, task_state state);

#endif
