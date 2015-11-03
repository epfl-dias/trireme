#ifndef __PARTITION_H_
#define __PARTITION_H_

#include <sys/queue.h>

#include "hashprotocol.h"
#include "util.h"

/**
 * Hash Table Storage Data Structures
 * struct elem       - element in table
 * struct bucket     - a bucket in a partition
 * struct partition  - hash table partition for server
 */
struct elem {
  // size must be 64 bytes
  hash_key key;
  size_t size;
  uint64_t ref_count;
  TAILQ_ENTRY(elem) chain;
  char *value;
  uint64_t local_values[2];
};

TAILQ_HEAD(elist, elem);

struct bucket {
  struct elist chain;
};

struct op_ctx {
  char is_local;
  char optype; 
  struct elem *e;
  void *old_value;
};

struct txn_ctx {
  int nops;
  struct op_ctx op_ctx[MAX_OPS_PER_QUERY];
};

struct partition {
  int nservers;
  int nhash;
  size_t nrecs;
  size_t size;
  struct bucket *table;

  struct txn_ctx txn_ctx;
  unsigned int seed;
  uint64_t q_idx;

  // stats
  int nhits;
  int ninserts;
  int nlookups_local;
  int nupdates_local;
  int naborts_local;

  int nlookups_remote;
  int nupdates_remote;
  int naborts_remote;

  uint64_t tps;

  uint64_t busyclock;
  uint64_t idleclock;

} __attribute__ ((aligned (CACHELINE)));

typedef void release_value_f(struct elem *e);

void init_hash_partition(struct partition *p, size_t max_size, int nservers);
void destroy_hash_partition(struct partition *p, release_value_f *release);

struct elem * hash_lookup(struct partition *p, hash_key key);
struct elem * hash_insert(struct partition *p, hash_key key, int size, release_value_f *release);
int hash_get_bucket(const struct partition *p, hash_key key);

#endif
