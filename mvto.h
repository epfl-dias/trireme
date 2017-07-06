struct elem *mvcc_acquire(struct partition *p, struct elem *e,
    char optype, uint64_t req_ts);

void mvcc_release(struct partition *p, struct elem *e_old, struct elem *e_new);
