#ifndef __SELOCK_H__
#define __SELOCK_H__

#include "partition.h"

int selock_acquire(struct partition *p, char optype, struct elem *e);
void selock_release(struct partition *p, struct elem *e);

#endif
