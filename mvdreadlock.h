#pragma once

void mvdreadlock_abort(struct task *ctask, struct hash_table *hash_table, int s);
struct elem *mvdreadlock_acquire(struct partition *p, struct elem *e, char optype);
int mvdreadlock_validate(struct task *ctask, struct hash_table *hash_table, int s);

