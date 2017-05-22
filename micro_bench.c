#include "headers.h"
#include "partition.h"
#include "smphashtable.h"
#include "benchmark.h"
#include "twopl.h"

void micro_load_data(struct hash_table *hash_table, int id)
{
  struct elem *e;
  uint64_t *value;
  struct partition *p = &hash_table->partitions[id];

#if SHARED_EVERYTHING
    // quick hack to get SE to load data from all threads
  uint64_t nrecs_per_server = p->nrecs / g_nservers;
  uint64_t qid = id * nrecs_per_server;
  uint64_t eqid = qid + nrecs_per_server;
#else
  uint64_t qid = id * p->nrecs;
  uint64_t eqid = qid + p->nrecs;
#endif

  printf("Partition %d loading start record %"PRIu64" to end"
          "record %"PRIu64"\n", id, qid, eqid);
  
  while (qid < eqid) {
    p->ninserts++;
    e = hash_insert(p, qid, YCSB_REC_SZ, NULL);
    assert(e);

    value = (uint64_t *)e->value;
    for (int i = 0; i < YCSB_NFIELDS; i++) 
      value[i] = 0;

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
  uint64_t nrecs_per_server = p->nrecs; 
  uint64_t nrecs = nrecs_per_server * g_nservers;

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
    assert(p->q_idx * s < g_niters * g_nservers);

    op->key = hash_table->keys[p->q_idx * s];

  } else if (g_alpha != 0) {

    /* Get key based on bernoulli distribution. In case of shared nothing 
     * and Trireme configs, there is one more parameter in addition to
     * nhot_recs, which is how hot records are spread out. 
     * 
     * In reality, under skew, we'll typically have just a few partitions 
     * store hot data. So we can use nhot_recs to limit #partitions for 
     * hot data.
     *
     * records 0 to nhot_recs are hot in each hot server.
     * nhot_servers are number of hot servers
     *
     */

    /* The two records we access remotely are hot records. The rest
     * are cold accesses
     */
    if (!is_local) {

      // pick random hot record
      delta = URand(&p->seed, 0, g_nhot_recs - 1);

      // pick random hot server
      tserver = URand(&p->seed, 0, g_nhot_servers - 1);
      
    } else {
      // pick random cold record
      delta = URand(&p->seed, g_nhot_recs + 1, nrecs_per_server - 1);

      // cold is always local
      tserver = s;
    }

    op->key = delta + (tserver * nrecs_per_server);

    //printf("srv(%d): nhps: %"PRIu64" - delta:%"PRIu64" - tserver:%d - opkey:%"PRIu64"\n", s, nhot_per_server, delta, tserver, op->key);

  } else {
    delta = URand(&p->seed, 0, nrecs_per_server - 1);
#if 0//ENABLE_DL_DETECT_CC
    tserver = URand(&p->seed, 0, g_nservers - 2);
#else
    tserver = URand(&p->seed, 0, g_nservers - 1);
#endif
    int t_coreid = hash_table->thread_data[tserver].core; 

#if ENABLE_SOCKET_LOCAL_TXN

    /* hacked to be specific to diascld33. if we are cores 0,1,2,3 then we 
     * want to do only remote txns. if we are any of the remaining cores
     * we want to do only socket local txns
     */
    if (is_local) {
      ; // local txn. don't bother changing tserver
    } else {
      if (is_first_core) {
        // only socket remote
        while ((t_coreid % 4) == (s_coreid % 4)) {
          tserver = URand(&p->seed, 0, g_nservers - 1);
          t_coreid = hash_table->thread_data[tserver].core; 
        }
      } else {
        // only socket local
        while ((tserver == s) || (t_coreid % 4) != (s_coreid % 4)) {
          tserver = URand(&p->seed, 0, g_nservers - 1);
          t_coreid = hash_table->thread_data[tserver].core; 
        }
      }
    }

#else
    while (!is_local && tserver == s) {
#if 0//ENABLE_DL_DETECT_CC
		tserver = URand(&p->seed, 0, g_nservers - 2);
#else
		tserver = URand(&p->seed, 0, g_nservers - 1);
#endif
      t_coreid = hash_table->thread_data[tserver].core; 
    }
#endif

    if (!is_local) {
      // remote op
      op->key = delta + (tserver * nrecs_per_server);

#if !defined (SHARED_NOTHING) && !defined(SHARED_EVERYTHING) && defined(ENABLE_SOCKET_LOCAL_TXN)
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
  uint64_t nrecs = p->nrecs; 
  uint64_t nrecs_per_server = g_nrecs / g_nservers;

  op->size = 0;

  // use zipfian if available
  if (hash_table->keys) {
    p->q_idx++;
    assert(p->q_idx * s < g_niters * g_nservers);

    op->key = hash_table->keys[p->q_idx * s];
  } else {
    
    // use alpha parameter as the probability
    if (g_alpha == 0) {
       // Just pick random key
        
#if MIGRATION
        hash_key delta;
        int tserver = s;
        while (!is_local && tserver == s) {
            tserver = URand(&p->seed, 0, g_nservers - 1);
        }
        delta = URand(&p->seed, 0, nrecs_per_server - 1);
        
        if (!is_local) {
            op->key = delta + (tserver * nrecs_per_server);
        } else {
            op->key = delta + (s * nrecs_per_server);
        }
#else
        op->key = URand(&p->seed, 0, g_nrecs - 1);
#endif
    } else {
      /* generate key based on bernoulli dist. alpha is probability
       * #items in one range = 0 to (nrecs * hot_fraction)
       * Based on probability pick right range
       */
        /*
      if (!is_local) {
        int t_server = URand(&p->seed, 0, g_nhot_servers - 1);
        op->key = t_server * nrecs_per_server + URand(&p->seed, 0, g_nhot_recs -1);
      } else {
        //cold range
          int t_server = URand(&p->seed, 0, g_nservers - 1);
        op->key = t_server * nrecs_per_server + URand(&p->seed, g_nhot_recs + 1, nrecs_per_server - 1);
      }
      */

        /* The two records we access remotely are hot records. The rest
         * are cold accesses
       */
        hash_key delta;
        int tserver;
        if (!is_local) {
            // pick random hot record
            delta = URand(&p->seed, 0, g_nhot_recs - 1);

            // pick random hot server
            tserver = URand(&p->seed, 0, g_nhot_servers - 1);
        } else {
            // pick random cold record
            delta = URand(&p->seed, g_nhot_recs + 1, nrecs_per_server - 1);

            // cold is always local
            tserver = s;
        }

        op->key = delta + (tserver * nrecs_per_server);
    }
  }
}

void micro_get_next_query(struct hash_table *hash_table, int s, void *arg)
{
  struct hash_query *query = (struct hash_query *) arg;
  struct partition *p = &hash_table->partitions[s];
  int r = URand(&p->seed, 1, 99);
  char is_local = (r < g_dist_threshold || g_nservers == 1);
  char is_duplicate;
  uint64_t nrecs_per_server = g_nrecs / g_nservers;
  
  query->nops = g_ops_per_txn;

  for (int i = 0; i < g_ops_per_txn; i++) {
    struct hash_op *op = &query->ops[i];

    if (URand(&p->seed, 1, 99) > g_write_threshold) {
        op->optype = OPTYPE_UPDATE;
    } else {
        op->optype = OPTYPE_LOOKUP;
    }

    do {
      // only first NREMOTE_OPS are remote in cross-partition txns
      if (is_local || (i >= g_nremote_ops && i < nrecs_per_server)) {
      //if (is_local || i >= g_nhot_recs) {
#if SHARED_EVERYTHING
        se_make_operation(hash_table, s, op, 1);
#else
        sn_make_operation(hash_table, s, op, 1 /* local op */);
#endif
      } else {
#if SHARED_EVERYTHING
        se_make_operation(hash_table, s, op, 0);
#else
        sn_make_operation(hash_table, s, op, 0 /* remote op */);
#endif
      }
      is_duplicate = FALSE;

      for (int j = 0; j < i; j++) {
        if (op->key == query->ops[j].key) {
          is_duplicate = TRUE;
          break;
        }
      }
    } while(is_duplicate == TRUE);

  }

#if ENABLE_KEY_SORTING
  // sort ops based on key
  for (int i = 0; i < g_ops_per_txn; i++) {
      for (int j = i + 1; j < g_ops_per_txn; j++) {
        if (query->ops[i].key > query->ops[j].key) {
            struct hash_op tmp = query->ops[i];
            query->ops[i] = query->ops[j];
            query->ops[j] = tmp;
        }
      }
  }
#endif

  p->q_idx++;
}

int micro_run_txn(struct hash_table *hash_table, int s, void *arg, 
    struct task *ctask)
{
  struct hash_query *query = (struct hash_query *) arg;
  struct txn_ctx *txn_ctx = &ctask->txn_ctx;
  int i, r;
  void *value;
  int nrecs_per_partition = hash_table->partitions[0].nrecs;

  txn_start(hash_table, s, txn_ctx);

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
    tserver = query->ops[i].key / nrecs_per_partition;
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

  for (i = 0; i < g_nservers; i++) {
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
    struct hash_op *op = &query->ops[i];
    
#if defined(MIGRATION)
    int tserver = (int)(op->key / (g_nrecs/g_nservers));
#else
    int tserver = ((int)(op->key)) / nrecs_per_partition;
#endif
    assert((tserver >= 0) && (tserver < g_batch_size * g_nservers));

//    printf("The target server for key %ld is %d (should be %d); nrec_per_partition = %d\n",
//    		op->key, tserver, (((int)(op->key))/nrecs_per_partition), nrecs_per_partition);
//    dprint("The target server for key %"PRIu64" is %d (%"PRIu64"); nrec_per_partition = %d\n",
//        		op->key, tserver, (op->key / nrecs_per_partition), nrecs_per_partition);
//
//    if ((op->key / nrecs_per_partition) != (((op->key))/nrecs_per_partition))
//    	assert(0);

    // try ith operation i+1 times before aborting only with NOWAIT CC
//#if ENABLE_WAIT_DIE_CC || defined(SHARED_NOTHING)
//    int nretries = 1;
//#else
//    int nretries = i + 1;
//#endif
//    for (int j = 0; j < nretries; j++) {
//      value = txn_op(ctask, hash_table, s, op, tserver);
//
//#if defined(MIGRATION)
//      s = ctask->s;
//      assert(s == get_affinity());
//#endif
//      if (value)
//        break;
//    }
//
//    if (!value) {
//      r = TXN_ABORT;
//      break;
//    }
    value = txn_op(ctask, hash_table, s, op, tserver);
#if defined(MIGRATION)
    s = ctask->s;
#endif
    if (!value) {
	  r = TXN_ABORT;
	  break;
	}

    // in both lookup and update, we just check the value
    uint64_t *int_val = (uint64_t *)value;

    for (int j = 1; j < YCSB_NFIELDS; j++) 
      assert(int_val[j] == int_val[0]);

    /*
    uint64_t val = int_val[0];
    assert(val == op->key);

    if (op->optype == OPTYPE_UPDATE) {
        int_val[0] = op->key;
        int_val[1]++;
    }
    */

    if (op->optype == OPTYPE_UPDATE) {
        for (int j = 0; j < YCSB_NFIELDS; j++) 
            int_val[j]++;
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
    r = txn_commit(ctask, hash_table, s, TXN_SINGLE);
  } else {
    txn_abort(ctask, hash_table, s, TXN_SINGLE);
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
};
