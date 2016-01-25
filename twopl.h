#pragma once

int no_wait_acquire(struct elem *e, char optype);
void no_wait_release(struct partition *p, struct elem *e);

#if ENABLE_WAIT_DIE_CC
int wait_die_acquire(int s, struct partition *p, int c, struct elem *e, 
    char optype, uint64_t req_ts, struct lock_entry **pl);
void wait_die_release(int s, struct partition *p, int c, struct elem *e);
#endif
