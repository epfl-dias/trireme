#ifndef __SELOCK_H__
#define __SELOCK_H__

int selock_acquire(struct elem *e, char optype);
void selock_release(struct elem *e);

#endif
