#pragma once

void svdreadlock_abort(struct task *ctask, struct hash_table *hash_table, int s);
struct elem *svdreadlock_acquire(struct partition *p, struct elem *e, char optype);
int svdreadlock_validate(struct task *ctask, struct hash_table *hash_table, int s);

