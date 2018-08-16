#pragma once

struct elem *mvto_acquire(struct partition *p, struct elem *e,
    char optype, uint64_t req_ts);

void mvto_release(struct partition *p, struct elem *e_old, struct elem *e_new);
