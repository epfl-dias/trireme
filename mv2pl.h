#pragma once

void mv2pl_abort(struct task *ctask, struct hash_table *hash_table, int s);
struct elem *mv2pl_acquire(struct partition *p, struct elem *e, char optype);
int mv2pl_validate(struct task *ctask, struct hash_table *hash_table, int s);
