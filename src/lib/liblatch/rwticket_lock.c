#include "headers.h"

#if RWTICKET_LOCK
void rwticket_init(rwticket_t *l)
{
    memset(l, 0, sizeof(rwticket_t));
}

void rwticket_wrunlock(rwticket_t *l)
{
  /*
    rwticket_t t = *l;
    
    __sync_synchronize();

    t.s.write++;
    t.s.read++;
    
    *(unsigned short *) l = t.us;
  */
}

int rwticket_wrtrylock(rwticket_t *l)
{
  /*
    unsigned me = l->s.users;
    unsigned char menew = me + 1;
    unsigned read = l->s.read << 8;
    unsigned cmp = (me << 16) + read + me;
    unsigned cmpnew = (menew << 16) + read + me;

    if (__sync_val_compare_and_swap(&l->u, cmp, cmpnew) == cmp) return 1;
    
    return 0;
  */
  return 0;
}


void rwticket_rdunlock(rwticket_t *l)
{
    __sync_add_and_fetch(&l->s.writers, 1);
}

int rwticket_rdtrylock(rwticket_t *lock)
{
    rwticket_t *l, old, new;

    l = lock;
    old = new = *l;

    if (old.s.readers != old.s.next)
      return 0;

    new.s.readers = new.s.next = old.s.next + 1;

    return __sync_bool_compare_and_swap(&l->u, old.u, new.u);
    
}

#endif
