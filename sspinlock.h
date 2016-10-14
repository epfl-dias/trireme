#pragma once

void sspinlock_init(sspinlock_t *l);
void sspinlock_acquire(sspinlock_t *l);
void sspinlock_release(sspinlock_t *l);
