#include "headers.h"
#include "partition.h"
#include "smphashtable.h"
#include "benchmark.h"

extern int ops_per_txn;
extern int batch_size;
extern int niters;
extern double dist_threshold;
extern double write_threshold;
extern double alpha;
extern double hot_fraction;
extern double nhot_servers;

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
  int nservers = hash_table->nservers;

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

#if SHARED_EVERYTHING
    // use zipfian if available
    if (hash_table->keys) {
      p->q_idx++;
      assert(p->q_idx * s < niters * nservers);

      op->key = hash_table->keys[p->q_idx * s];
    } else {
      uint64_t nrecs = hash_table->partitions[0].nrecs; 
      
      // use alpha parameter as the probability
      if (alpha == 0) {
        op->key = nrecs * r;
        op->key %= nrecs;
      } else {
        /* generate key based on bernoulli dist. alpha is probability
         * #items in one range = 0 to (nrecs * hot_fraction)
         * Based on probability pick right range
         */
        hash_key last_hot_rec  = nrecs * hot_fraction / 100;
        if (alpha > r) {
          //hot range
          op->key = last_hot_rec * r;
        } else {
          //cold range
          op->key = (nrecs - last_hot_rec) * r + last_hot_rec;
        }
      }
    }

#else

    hash_key delta = 0;
    int tserver = 0;

    /* we can either do zipfian across all partitions or explicitly control
     * local vs remote. Doing both is meaningless - zipfian within a partition
     * is not zipfian globally
     */
    if (hash_table->keys) {
      p->q_idx++;
      assert(p->q_idx * s < niters * nservers);

      op->key = hash_table->keys[p->q_idx * s];

    } else if (alpha != 0) {

      /* Get key based on bernoulli distribution. In case of shared nothing 
       * and Trireme configs, there is one more parameter in addition to
       * alpha - the probability, and hot_fraction, which is how hot and 
       * cold records are spread out. 
       * Under skew, we'll typically have just a few partitions store hot
       * data. So we can use hot_fraction to limit #partitions for hot data.
       * We assume cold data is always local
       */
      hash_key last_hot_rec  = p->nrecs * hot_fraction / 100;

      if (alpha > r) {
        // hot data
        delta = last_hot_rec * r;
        tserver = nhot_servers * r;
      } else {
        // cold data
        delta = (p->nrecs - last_hot_rec) * r + last_hot_rec;
        tserver = ((hash_key) (nservers * r)) % nservers;
      }
    } else {
      delta = (hash_key) p->nrecs * r;
      tserver = ((hash_key) (nservers * r)) % nservers;
    }

    if (!is_local) {
      // remote op
      op->key = delta + (tserver * p->nrecs);
    } else {
      //local op
      op->key = delta + (s * p->nrecs);
    }

#endif
  }
}

int micro_run_txn(struct hash_table *hash_table, int s, void *arg)
{
  struct hash_query *query = (struct hash_query *) arg;
  int i, r;
  void *value;

  txn_start(hash_table, s);

#if SHARED_NOTHING
  /* we have to acquire all partition locks in partition order. This protocol
   * is similar to the partitioned store design used in silo. 
   */
  
  char partitions[BITNSLOTS(MAX_SERVERS)];
  int tserver;

  memset(partitions, 0, sizeof(partitions));

  for (i = 0; i < query->nops; i++) {
    tserver = micro_hash_get_server(hash_table, query->ops[i].key);
    BITSET(partitions, tserver);

    if (tserver != s)
      hash_table->partitions[s].nlookups_remote++;
  }

  for (i = 0; i < hash_table->nservers; i++) {
    if (BITTEST(partitions, i)) {
      LATCH_ACQUIRE(&hash_table->partitions[i].latch);
    }
  }

#endif

  r = TXN_COMMIT;
  for (i = 0; i < query->nops; i++) {
    struct hash_op *op = &query->ops[i];
    int is_local;
#if defined SHARED_EVERYTHING || defined SHARED_NOTHING
    is_local = 1;
#else
    is_local = (micro_hash_get_server(hash_table, op->key) == s);
#endif

#if SHARED_NOTHING
    /* in shared nothing case, we locate the partition, latch it
     * and then run the transaction
     */
    tserver = micro_hash_get_server(hash_table, op->key);
    struct partition *tpartition = &hash_table->partitions[tserver];

    value = txn_op(hash_table, s, tpartition, op, is_local);
    assert(value);
#else
    value = txn_op(hash_table, s, NULL, op, is_local);
#endif
  
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

#if SHARED_NOTHING
  /* in shared nothing case, there is now way a transaction will ever be 
   * aborted. We always get all partition locks before we start. So txn
   * execution is completely serial. Likewise, there is no need to reset
   * reference counts in records for logical locks as we don't need them.
   *
   * So just unlatch the partitions and be done
   */

  assert (r == TXN_COMMIT);
  for (i = 0; i < hash_table->nservers; i++) {
    if (BITTEST(partitions, i)) {
      LATCH_RELEASE(&hash_table->partitions[i].latch);
    }
  }
#else

  if (r == TXN_COMMIT) {
    txn_commit(hash_table, s, TXN_SINGLE);
  } else {
    txn_abort(hash_table, s, TXN_SINGLE);
  }
#endif

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
