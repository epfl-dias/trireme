#pragma once

int silo_validate(struct task *ctask, struct hash_table *hash_table, int s);
void silo_latch_acquire(struct elem *e);
void silo_latch_release(struct elem *e);

#define SILO_LOCK_BIT ((1UL << 63))

#ifndef SILO_USE_ATOMICS
#define SILO_USE_ATOMICS 1
#endif
