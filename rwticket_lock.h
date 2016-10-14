#pragma once

void rwticket_init(rwticket_t *lock);
int rwticket_wrtrylock(rwticket_t *l);
int rwticket_rdtrylock(rwticket_t *l);
void rwticket_rdlock(rwticket_t *l);
void rwticket_wrunlock(rwticket_t *l);
void rwticket_rdunlock(rwticket_t *l);

