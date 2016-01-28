#include "headers.h"
#include "partition.h"
#include "smphashtable.h"
#include "benchmark.h"
#include "twopl.h"

extern int ops_per_txn;
extern int batch_size;
extern int niters;
extern int dist_threshold;
extern int write_threshold;
extern int alpha;
extern size_t nhot_recs;
extern int nhot_servers;

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

  uint64_t qid = id * p->nrecs;
  uint64_t eqid = qid + p->nrecs;
  
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

static void sn_make_operation(struct hash_table *hash_table, int s, 
    struct hash_op *op, char is_local)
{
  struct partition *p = &hash_table->partitions[s];
  int nservers = hash_table->nservers;
#if SHARED_EVERYTHING
  uint64_t nrecs = p->nrecs; 
  uint64_t nrecs_per_server = nrecs / nservers;
#else
  uint64_t nrecs_per_server = p->nrecs; 
  uint64_t nrecs = nrecs_per_server * nservers;
#endif
  int r = URand(&p->seed, 1, 99);
 
  if (r > write_threshold) {
    op->optype = OPTYPE_UPDATE;
  } else {
    op->optype = OPTYPE_LOOKUP;
  }
  op->size = 0;

  hash_key delta = 0;
  int tserver = 0;

  // hacked to be specific to diascld33
  int s_coreid = hash_table->thread_data[s].core;
  int is_first_core = s_coreid < 4; //0,1,2,3 are first cores on 4 sockets

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
     * alpha and nhot_recs, which is how hot records are spread out. 
     * 
     * In reality, under skew, we'll typically have just a few partitions 
     * store hot data. So we can use nhot_recs to limit #partitions for 
     * hot data.
     *
     * records 0 to hot_recs_per_server are hot in each hot server.
     *
     */
    uint64_t nhot_per_server = nhot_recs / nhot_servers;

    if (alpha > r) {

      // XXX: BROKEN FIX
      assert(0);

      // hot data
      delta = URand(&p->seed, 0, nhot_recs - 1);
      tserver = delta / nhot_per_server;
      
#if ENABLE_SOCKET_LOCAL_TXN
      delta = URand(&p->seed, 0, nhot_per_server - 1);
      tserver = URand(&p->seed, 0, nhot_servers - 1);
      int t_coreid = hash_table->thread_data[tserver].core; 

      // hacked to be specific to diascld33
      // allow first core of each socket to do cross-socket txns
      while (!is_first_core && t_coreid % 4 != s_coreid % 4) {
        delta = URand(&p->seed, 0, nhot_per_server - 1);
        tserver = URand(&p->seed, 0, nhot_servers - 1);
        t_coreid = hash_table->thread_data[tserver].core; 
      }
#endif
    } else {
      // cold data
      size_t ncold_recs = p->nrecs * nservers - nhot_recs;
      uint64_t ncold_per_server = p->nrecs - nhot_per_server;
      
      // pick random cold record
      delta = URand(&p->seed, nhot_per_server + 1, p->nrecs - 1);

      // pick random server
      tserver = URand(&p->seed, 0, nservers - 1);

#if ENABLE_SOCKET_LOCAL_TXN
      delta = URand(&p->seed, nhot_per_server + 1, p->nrecs - 1);
      tserver = URand(&p->seed, 0, nservers - 1);

      int t_coreid = hash_table->thread_data[tserver].core; 

      // hacked to be specific to diascld33
      while (!is_first_core && t_coreid % 4 != s_coreid % 4) {
        tserver = URand(&p->seed, 0, nservers - 1);
        t_coreid = hash_table->thread_data[tserver].core; 
      }
#endif
    }

#if ENABLE_SOCKET_LOCAL_TXN
    op->key = delta + (tserver * p->nrecs);
#else
    op->key = delta;
#endif

    //printf("srv(%d): nhps: %"PRIu64" - delta:%"PRIu64" - tserver:%d - opkey:%"PRIu64"\n", s, nhot_per_server, delta, tserver, op->key);

  } else {
    delta = URand(&p->seed, 0, nrecs_per_server - 1);
    tserver = URand(&p->seed, 0, nservers - 1);
    int t_coreid = hash_table->thread_data[tserver].core; 

#if ENABLE_SOCKET_LOCAL_TXN

    // hacked to be specific to diascld33
    while ((!is_local && tserver == s) || 
        (!is_first_core && t_coreid % 4 != s_coreid % 4)) {
#else
    while (!is_local && tserver == s) {
#endif
      tserver = URand(&p->seed, 0, nservers - 1);
      t_coreid = hash_table->thread_data[tserver].core; 
    }

    if (!is_local) {
      // remote op
      op->key = delta + (tserver * nrecs_per_server);

#if !defined (SHARED_NOTHING) && !defined(SHARED_EVERYTHING) && ENABLE_SOCKET_LOCAL_TXN == 1
      if (hash_table->thread_data[tserver].core % 4 !=  s_coreid % 4) {
        assert(is_first_core);
      }
#endif

    } else {
      //local op
      op->key = delta + (s * nrecs_per_server);
    }
  }
}

static void se_make_operation(struct hash_table *hash_table, int s, 
    struct hash_op *op, char is_local)
{
  struct partition *p = &hash_table->partitions[s];
  int nservers = hash_table->nservers;
  uint64_t nrecs = p->nrecs; 
  uint64_t nrecs_per_server = nrecs / nservers;
  int r = URand(&p->seed, 1, 99);
 
  if (r > write_threshold) {
    op->optype = OPTYPE_UPDATE;
  } else {
    op->optype = OPTYPE_LOOKUP;
  }
  op->size = 0;

  // use zipfian if available
  if (hash_table->keys) {
    p->q_idx++;
    assert(p->q_idx * s < niters * nservers);

    op->key = hash_table->keys[p->q_idx * s];
  } else {
    
    // use alpha parameter as the probability
    if (alpha == 0) {

      /* dist_threshold can be used to logically afinitize thread to data. 
       * this will maximize cache utilization
       */
      if (is_local) {
        // always pick within this server's key range
        hash_key delta = URand(&p->seed, 0, nrecs_per_server - 1);
        op->key = s * nrecs_per_server + delta;
      } else {
        // just pick randomly
        op->key = URand(&p->seed, 0, nrecs - 1);
      }
    } else {
      /* generate key based on bernoulli dist. alpha is probability
       * #items in one range = 0 to (nrecs * hot_fraction)
       * Based on probability pick right range
       */
      if (alpha > r) {
        //hot range
        op->key = URand(&p->seed, 0, nhot_recs - 1);
      } else {
        //cold range
        op->key = URand(&p->seed, nhot_recs + 1, nrecs - 1);
      }
    }
  }
}

void micro_get_next_query(struct hash_table *hash_table, int s, void *arg)
{
  struct hash_query *query = (struct hash_query *) arg;
  struct partition *p = &hash_table->partitions[s];
  int r = URand(&p->seed, 1, 99);
  char is_local = (r < dist_threshold || hash_table->nservers == 1);
  char is_duplicate;
  
  query->nops = ops_per_txn;
  query->npending = 0;

  for (int i = 0; i < ops_per_txn; i++) {
    struct hash_op *op = &query->ops[i];

    do {
//#if SHARED_EVERYTHING
//      se_make_operation(hash_table, s, op, is_local);
//#else
      // if txn is distributed, then first NREMOTE_OPS are remote
      if (is_local || i >= 2)
        sn_make_operation(hash_table, s, op, 1 /* local op */);
      else
        sn_make_operation(hash_table, s, op, 0 /* remote op */);
//#endif
      is_duplicate = FALSE;

      for (int j = 0; j < i; j++) {
        if (op->key == query->ops[j].key) {
          is_duplicate = TRUE;
          break;
        }
      }
    } while(is_duplicate == TRUE);

  }

  p->q_idx++;
}

#if PARTITION_LOCK_MODE
int micro_run_txn_with_plock(struct hash_table *hash_table, int s, void *arg, 
    int status)
{
  struct hash_query *query = (struct hash_query *) arg;
  int i, r;
  void *value;
  struct lock_entry *l;
  char bits[BITNSLOTS(MAX_SERVERS)], nbits;
  struct hash_op op, *p_op;
  int nservers = hash_table->nservers;
  struct partition *p = &hash_table->partitions[s];

  op.optype = OPTYPE_PLOCK_ACQUIRE;
  op.key = 0;
  op.size = 0;
  p_op = &op;

  memset(bits, 0, sizeof(bits));

  txn_start(hash_table, s, status);

  for (i = 0; i < query->nops; i++) {
    struct hash_op *op = &query->ops[i];
    int tserver = micro_hash_get_server(hash_table, op->key);
    struct partition *tpartition = &hash_table->partitions[tserver];
    r = TXN_COMMIT;

    // if we already have partition lock, skip to getting data
    if (BITTEST(bits, tserver))
      goto process_op;
        
    /* we dont have partition lock. try to get it */
    if (tserver == s) {
      // local access
#if ENABLE_WAIT_DIE_CC
      r = wait_die_acquire(s, p, s, &p->magic_elem, OPTYPE_UPDATE, 
          p->txn_ctx.ts, &l);
#else
      r = no_wait_acquire(&p->magic_elem, OPTYPE_UPDATE);
#endif
    
      if (r == LOCK_SUCCESS) {
        r = TXN_COMMIT; // great we have the lock
      } else if (r == LOCK_WAIT) {
        /* we have to spin now until value is ready. But we also need to
         * service other requests
         */
#if !defined(ENABLE_WAIT_DIE_CC)
        assert(0);
#endif
        assert(l);
        while (!l->ready)
          process_requests(hash_table, s);

        r = TXN_COMMIT;
      } else {
        // busted
        assert(r == LOCK_ABORT);
        r = TXN_ABORT;
        p->naborts_local++;
        break;
      }

    } else {
      // remote partn. send msg
      p_op->key = tserver;
      smp_hash_doall(hash_table, s, 1, &p_op, &value);
      if (!value) {
        p->naborts_remote++;
        r = TXN_ABORT;
        break;
      } else {
        assert((((uint64_t)value) & ~DATA_READY_MASK) == 2);
      }
    }

    assert (r == TXN_COMMIT);

    // mark server's partition as locked so that we can free later 
    BITSET(bits, tserver);

process_op:
    /* now that we have the lock, we can issue the actual operation as though 
     * its local
     */
    value = txn_op(hash_table, s, tpartition, op, 1);
    assert(value);

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

  // release all partition locks
  for (i = 0; i < nservers; i++) {
    if (BITTEST(bits, i)) {
      if (i == s) {
#if ENABLE_WAIT_DIE_CC
        wait_die_release(s, p, s, &p->magic_elem);
#else
        no_wait_release(&hash_table->partitions[s], &p->magic_elem);
#endif
      } else {
        mp_release_plock(s, i);
      }
    }
  }

  return r;
}
#endif

int micro_run_txn(struct hash_table *hash_table, int s, void *arg, struct txn_ctx *txn_ctx, 
    int status)
{
  struct hash_query *query = (struct hash_query *) arg;
  int i, r;
  void *value;

  txn_start(hash_table, s, status, txn_ctx);

#if SHARED_NOTHING
  /* we have to acquire all partition locks in partition order. This protocol
   * is similar to the partitioned store design used in silo. 
   * We do this only if we know there are remote transactions
   */
  
  int tserver, alock_state, partitions[MAX_SERVERS], npartitions;
  char bits[BITNSLOTS(MAX_SERVERS)], nbits;

  memset(bits, 0, sizeof(bits));
  npartitions = nbits = 0;

  for (i = 0; i < query->nops; i++) {
    tserver = micro_hash_get_server(hash_table, query->ops[i].key);
    if (BITTEST(bits, tserver)) {
      ;
    } else {
      BITSET(bits, tserver);
      nbits++;
    }

#if GATHER_STATS
    if (tserver != s)
      hash_table->partitions[s].nlookups_remote++;
#endif
  }

  for (i = 0; i < hash_table->nservers; i++) {
    if (BITTEST(bits, i)) {
      dprint("srv %d latching lock %d\n", s, i);
      LATCH_ACQUIRE(&hash_table->partitions[i].latch, &alock_state);
      dprint("srv %d got lock %d\n", s, i);
      partitions[npartitions++] = i;
      nbits--;
    }

    if (nbits == 0)
      break;
  }

#endif

  r = TXN_COMMIT;
  for (i = 0; i < query->nops; i++) {
    query->npending = 1;
    query->pending_ops[0] = i;

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

    value = txn_op(hash_table, s, tpartition, op, is_local, txn_ctx);
    assert(value);
#else
    // try ith operation i+1 times before aborting only with NOWAIT CC
#if ENABLE_WAIT_DIE_CC
    int nretries = 1;
#else
    int nretries = i + 1;
#endif
    for (int j = 0; j < nretries; j++) {
      value = txn_op(hash_table, s, NULL, op, is_local, txn_ctx);
      if (value)
        break;
    }
#endif //shared_nothing
  
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

  for (i = 0; i < npartitions; i++) {
    int t = partitions[i];
    assert (BITTEST(bits, t)); 
    LATCH_RELEASE(&hash_table->partitions[t].latch, &alock_state);
    dprint("srv %d releasing lock %d\n", s, i);
  }
#else

  if (r == TXN_COMMIT) {
    txn_commit(hash_table, s, TXN_SINGLE, txn_ctx);
  } else {
    txn_abort(hash_table, s, TXN_SINGLE, txn_ctx);
  }
#endif

  return r;
}

struct benchmark micro_bench = {
  .load_data = micro_load_data,
  .alloc_query = micro_alloc_query,
  .get_next_query = micro_get_next_query,
#if PARTITION_LOCK_MODE
  .run_txn = micro_run_txn_with_plock,
#else
  .run_txn = micro_run_txn,
#endif
  .verify_txn = NULL,
  .hash_get_server = micro_hash_get_server,
};
