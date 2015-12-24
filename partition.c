#include <assert.h>
#include <malloc.h>
#include <math.h>
#include <pthread.h>
#include <stdlib.h>
#include <string.h>
#include <inttypes.h>
#include <sys/queue.h>

#include "hashprotocol.h"
#include "partition.h"
#include "util.h"
#include "tpcc.h"

void init_hash_partition(struct partition *p, size_t nrecs, int nservers)
{
  assert((unsigned long)p % CACHELINE == 0);
  p->nservers = nservers;
  p->nrecs = nrecs;
  p->size = 0;

  // below is a trick to make GCD of p->nhash and nservers equal to 1
  // it can be proved that if GCD of nhash and nservers is 1 then, hash_get_server and
  // hash_get_bucket will be unbiased when input is random
  // assume each bucket on avg holds 2 elements
  p->nhash = ceil((double)max(10.0, nrecs / 2) / nservers) * nservers - 1;

  p->q_idx = 0;
  p->nhits = 0;
  p->ninserts = 0;
  p->nlookups_local = 0;
  p->nupdates_local = 0;
  p->naborts_local = 0;

  p->nlookups_remote = 0;
  p->nupdates_remote = 0;
  p->naborts_remote = 0;

  p->busyclock = 0;
  p->idleclock = 0;
  p->seed = rand();

  p->table = memalign(CACHELINE, p->nhash * sizeof(struct bucket));
  assert((unsigned long) &(p->table[0]) % CACHELINE == 0);
  for (int i = 0; i < p->nhash; i++) {
    TAILQ_INIT(&(p->table[i].chain));
  }
}

void destroy_hash_partition(struct partition *p, release_value_f *release)
{
  size_t dbg_p_size = 0;
  for (int i = 0; i < p->nhash; i++) {
    struct elist *eh = &p->table[i].chain;
    struct elem *e = TAILQ_FIRST(eh);
    while (e != NULL) {
      struct elem *next = TAILQ_NEXT(e, chain);
      dbg_p_size += sizeof(struct elem) + e->size;
      release(e);
      e = next;
    }
  }

  assert(p->size == dbg_p_size);
  free(p->table);
}

/**
 * hash_get_bucket: returns bucket were given key is or should be placed
 */
int hash_get_bucket(const struct partition *p, hash_key key)
{
  return key % p->nhash;
}

void hash_remove(struct partition *p, struct elem *e)
{
  struct elist *eh = &(p->table[hash_get_bucket(p, e->key)].chain);
  p->size -= (sizeof(struct elem) + e->size);
  assert(p->size >= 0);
  TAILQ_REMOVE(eh, e, chain);
  free(e->value);
  free(e);

  dprint("Deleted %"PRId64"\n", e->key);
}

struct elem * hash_lookup(struct partition *p, hash_key key)
{
  struct elist *eh = &(p->table[hash_get_bucket(p, key)].chain);
  struct elem *e = TAILQ_FIRST(eh);

  while (e != NULL) {
    if (e->key == key) {
      return e;
    }
    e = TAILQ_NEXT(e, chain);
  }


  return NULL;
}

struct elem *hash_insert(struct partition *p, hash_key key, int size, 
        release_value_f *release)
{  
  struct elist *eh = &(p->table[hash_get_bucket(p, key)].chain);
  struct elem *e;
  
#if VERIFY_CONSISTENCY
  e = hash_lookup(p, key);
  assert (e == NULL || (key & ORDER_TID));
#endif

  // try to allocate space for new value
  e = (struct elem *)memalign(CACHELINE, sizeof(struct elem));
  assert (e);

  e->key = key;

  // if size fits locally, store locally. Else alloc
  if (size < sizeof(e->local_values)) 
    e->value = (char *)e->local_values;
  else
    e->value = malloc(size);
  assert(e->value);

  e->size = size;
  p->size += sizeof(struct elem) + e->size;

  TAILQ_INSERT_TAIL(eh, e, chain);
  
  return e;
}

