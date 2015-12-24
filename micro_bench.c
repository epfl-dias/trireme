#include <stdio.h>
#include <string.h>
#include <assert.h>
#include <stdlib.h>
#include <inttypes.h>
#include "onewaybuffer.h"
#include "partition.h"
#include "smphashtable.h"
#include "util.h"
#include "benchmark.h"
#include "tpcc.h"

extern int ops_per_txn;
extern int batch_size;
extern int niters;
extern double dist_threshold;
extern double write_threshold;
extern double alpha;

int micro_hash_get_server(struct hash_table *hash_table, hash_key key)
{
  //return key % hash_table->nservers;
  return key / hash_table->nrecs;
}


void micro_load_data(struct hash_table *hash_table, int id)
{
  struct elem *e;
  uint64_t *value;
  struct partition *p = &hash_table->partitions[id];

  size_t qid = id * p->nrecs;
  size_t eqid = qid + p->nrecs;
  
  while (qid < eqid) {
    p->ninserts++;
    e = hash_insert(p, qid, YCSB_REC_SZ, NULL);
    assert(e);

    value = (uint64_t *)e->value;
    for (int i = 0; i < YCSB_NFIELDS; i++) 
      value[i] = qid;

    e->ref_count = 1;

    qid++;
  }
}

void *micro_alloc_query()
{
  struct hash_query *q = malloc(sizeof(struct hash_query));
  assert(q);

  return (void *)q;
}

void micro_get_next_query(struct hash_table *hash_table, int s, void *arg)
{
  char is_local;
  struct hash_query *query = (struct hash_query *) arg;
  struct partition *p = &hash_table->partitions[s];
  double r = ((double)rand_r(&p->seed) / RAND_MAX);

  is_local = (r < dist_threshold);

  query->nops = ops_per_txn;

  for (int i = 0; i < ops_per_txn; i++) {
    struct hash_op *op = &query->ops[i];

    r = ((double)rand_r(&p->seed) / RAND_MAX);

    if (r > write_threshold) {
      op->optype = OPTYPE_UPDATE;
    } else {
      op->optype = OPTYPE_LOOKUP;
    }
    op->size = 0;

    unsigned long delta;
    if (hash_table->keys)
      delta = hash_table->keys[p->q_idx++];
    else
      delta = (unsigned long) p->nrecs * r;

    if (!is_local) {
      // remote op
      op->key = delta + (((s + 1) % hash_table->nservers) * p->nrecs);
      //op->key = delta + p->nrecs; /* all reqs to server 1 */
    } else {
      //local op
      op->key = delta + (s * p->nrecs);
    }
  }
}

int micro_run_txn(struct hash_table *hash_table, int s, void *arg)
{
  struct hash_query *query = (struct hash_query *) arg;
  int i, r;
  void *value;

  txn_start(hash_table, s);

  r = TXN_COMMIT;
  for (i = 0; i < query->nops; i++) {
    struct hash_op *op = &query->ops[i];
    int is_local = (micro_hash_get_server(hash_table, op->key) == s);

    value = txn_op(hash_table, s, NULL, op, is_local);
  
    if (!value) {
      r = TXN_ABORT;
      break;
    }

    // in both lookup and update, we just check the value
    uint64_t *int_val = (uint64_t *)value;

    for (int j = 0; j < YCSB_NFIELDS; j++) {
      assert (int_val[j] == op->key);
    }
  }

  if (r == TXN_COMMIT) {
    txn_commit(hash_table, s, TXN_SINGLE);
  } else {
    txn_abort(hash_table, s, TXN_SINGLE);
  }

  return r;
}

struct benchmark micro_bench = {
  .load_data = micro_load_data,
  .alloc_query = micro_alloc_query,
  .get_next_query = micro_get_next_query,
  .run_txn = micro_run_txn,
  .verify_txn = NULL,
  .hash_get_server = micro_hash_get_server,
};
