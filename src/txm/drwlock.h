#pragma once

#if DRW_LOCK
void drwlock_init(drwlock_t *l);
void drwlock_wrunlock(drwlock_t *l);
int drwlock_wrtrylock(drwlock_t *l);
void drwlock_rdunlock(int s, drwlock_t *l);
int drwlock_rdtrylock(int s, drwlock_t *l);
#endif
