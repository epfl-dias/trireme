#ifndef __SELOCK_H__
#define __SELOCK_H__

int selock_acquire(struct partition *p, struct elem *e, char optype, uint64_t ts);
void selock_release(struct partition *p, struct op_ctx *ctx);

#endif
