#pragma once

void rwlock_init(rwlock_t *l);
void rwlock_wrunlock(rwlock_t *l);
int rwlock_wrtrylock(rwlock_t *l);
void rwlock_rdunlock(rwlock_t *l);
int rwlock_rdtrylock(rwlock_t *l);
