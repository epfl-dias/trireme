#include <sys/sysinfo.h>

#include "headers.h"
#include "onewaybuffer.h"
#include "partition.h"
#include "smphashtable.h"
#include "benchmark.h"
#include "tpcc.h"
#include "plmalloc.h"
#include "twopl.h"

static volatile int nready = 0;
const char *optype_str[] = {"","lookup","insert","update","release"};
#define OPTYPE_STR(optype) optype_str[optype >> 60]

extern struct benchmark *g_benchmark;
extern double alpha;
extern int niters;
extern int batch_size;
extern int ops_per_txn, nhot_servers;
extern struct hash_table *hash_table;

// Forward declarations
void *hash_table_server(void* args);
int is_value_ready(struct elem *e);
void smp_hash_doall(struct task *ctask, struct hash_table *hash_table,
    int client_id, int server, int nqueries, struct hash_op **queries, 
    void **values);
int smp_hash_lookup(struct task *ctask, struct hash_table *hash_table, 
    int client_id, int server, hash_key key, short opid);
int smp_hash_update(struct task *ctask, struct hash_table *hash_table, 
    int client_id, int server, hash_key key, short opid);

struct hash_table *create_hash_table(size_t nrecs, int nservers)
{
  struct hash_table *hash_table = (struct hash_table *)malloc(sizeof(struct hash_table));
  hash_table->keys = NULL;
  hash_table->nservers = nservers;
  hash_table->nrecs = nrecs / nservers;
  hash_table->partitions = memalign(CACHELINE, nservers * sizeof(struct partition));
  hash_table->g_partition = memalign(CACHELINE, sizeof(struct partition));

  hash_table->nclients = 0;
  pthread_mutex_init(&hash_table->create_client_lock, NULL);

  hash_table->boxes = memalign(CACHELINE, MAX_CLIENTS * sizeof(struct box_array));
  for (int i = 0; i < hash_table->nservers; i++) {

#if SHARED_EVERYTHING
    init_hash_partition(&hash_table->partitions[i], nrecs, nservers, 
        i == 0 /*alloc buckets only for first partition*/);

    /* make other partition buckets point to first partition's buckets */
    if (i > 0) 
      hash_table->partitions[i].table = hash_table->partitions[0].table;

#else

    init_hash_partition(&hash_table->partitions[i], nrecs / nservers, nservers, 
        1 /*always alloc buckets*/);

#endif
  }

  init_hash_partition(hash_table->g_partition, nrecs / nservers, nservers, 1);

  hash_table->threads = (pthread_t *)malloc(nservers * sizeof(pthread_t));
  hash_table->thread_data = (struct thread_args *)malloc(nservers * sizeof(struct thread_args));

  create_hash_table_client(hash_table);

  /*
  if (alpha != 0) {
    printf("Generating zipfian distribution: ");
    fflush(stdout);

    hash_table->keys = zipf_get_keys(alpha, nrecs, niters * nservers);
  } else
    hash_table->keys = NULL;
  */

  return hash_table;
}

void destroy_hash_table(struct hash_table *hash_table)
{
  int i;
  size_t act_psize, dbg_psize;
  act_psize = 0;

#if SHARED_EVERYTHING
  /* call destroy partition on first partition only for shared everything case
   * In shared nothing and trireme, partition destruction happens in each 
   * thread.
   */
  for (i = 0; i < hash_table->nservers; i++) {
    act_psize += hash_table->partitions[i].size;
  }

  dbg_psize = destroy_hash_partition(&hash_table->partitions[0]);
  assert(act_psize == dbg_psize);

#endif

  /* XXX: What about the global partition? */
  //destroy_hash_partition(&hash_table->g_partition, atomic_release_value_);

  free(hash_table->partitions);
  free(hash_table->g_partition);

  for (int i = 0; i < hash_table->nservers; i++) 
    free(hash_table->boxes[i].boxes);
  
  free(hash_table->boxes);

  free(hash_table->threads);
  free(hash_table->thread_data);
  free(hash_table);
}

void start_hash_table_servers(struct hash_table *hash_table, int first_core) 
{
  int r;
  void *value;
  hash_table->quitting = 0;
  int ncpus, coreid, ncores, socketid;

  ncores = coreid = socketid = 0;

  ncpus = get_nprocs_conf() / 2;
  assert(ncpus >= hash_table->nservers);

  for (int i = 0; i < hash_table->nservers; i++) {
    hash_table->thread_data[i].id = i;
    hash_table->thread_data[i].core = coreid;
    hash_table->thread_data[i].hash_table = hash_table;

    printf("Assinging core %d to srv %d\n", coreid, i);

    r = pthread_create(&hash_table->threads[i], NULL, hash_table_server, (void *) (&hash_table->thread_data[i]));
    assert(r == 0);

    coreid += 4;
    ncores++;
    if (ncores == 18) {
      ncores = 0;
      coreid = ++socketid;
    }
  }

  for (int i = 0; i < hash_table->nservers; i++) {
    r = pthread_join(hash_table->threads[i], &value);
    assert(r == 0);
  }
}

void stop_hash_table_servers(struct hash_table *hash_table)
{
  int r;
  void *value;

  // flush all client buffers
  for (int i = 0; i < hash_table->nservers; i++) {
    for (int k = 0; k < hash_table->nclients; k++) {
      buffer_flush(&hash_table->boxes[k].boxes[i].in);
    }
  }

  hash_table->quitting = 1;
  for (int i = 0; i < hash_table->nservers; i++) {
    r = pthread_join(hash_table->threads[i], &value);
    assert(r == 0);
  }
}

void create_hash_table_client(struct hash_table *hash_table)
{
  for (int i = 0; i < hash_table->nservers; i++) {
    hash_table->boxes[i].boxes = memalign(CACHELINE, hash_table->nservers * sizeof(struct box));
    assert((unsigned long) &hash_table->boxes[i] % CACHELINE == 0);
    
    for (int j = 0; j < hash_table->nservers; j++) {
      memset((void*)&hash_table->boxes[i].boxes[j], 0, sizeof(struct box));
      assert((unsigned long) &hash_table->boxes[i].boxes[j].in % CACHELINE == 0);
      assert((unsigned long) &hash_table->boxes[i].boxes[j].out % CACHELINE == 0);
    }
  }
}

struct elem *local_txn_op(struct task *ctask, int s, struct txn_ctx *ctx, 
    struct partition *p, struct hash_op *op)
{
  struct elem *e;
  uint32_t t = op->optype;
  struct lock_entry *l = NULL;
  int r;

  switch (t) {
    case OPTYPE_INSERT:
      // should not get a insert to item partition
      assert (p != hash_table->g_partition);

      e = hash_insert(p, op->key, op->size, NULL);
      assert(e);

      // set ref count to 1 here so that cc algo will set it appropriate below
      e->ref_count = 1;
      p->ninserts++;

#if SHARED_EVERYTHING
      if (!selock_acquire(p, e, t, ctx->ts)) {
        p->naborts_local++;
        return NULL;
      }
#elif SHARED_NOTHING
      // no logical locks. do nothing
#else
#if ENABLE_WAIT_DIE_CC
      r = wait_die_acquire(s, p, s /* client id */, ctask->tid, 0 /* opid */, 
          e, t, ctx->ts, &l);
#else
      r = no_wait_acquire(e, t);
#endif
      // insert can only succeed
      assert (r == LOCK_SUCCESS);
#endif

      break;

    case OPTYPE_LOOKUP:
    case OPTYPE_UPDATE:
      e = hash_lookup(p, op->key);
      if (!e) {
        dprint("srv(%d): lookup key %"PRIu64"failed\n", s, op->key);
        assert(0);
      }

      // if this is the ITEM TID, we are done
      if (p == hash_table->g_partition)
        break;

#if SHARED_EVERYTHING
      if (!selock_acquire(p, e, t, ctx->ts)) {
        p->naborts_local++;
        return NULL;
      }
#elif SHARED_NOTHING
      /* Do absolutely nothing for shared nothing as it proceeds by first
       * getting partition locks. So there is no need to get record locks
       * as access to the whole partition itself is serialized
       */
#else

#if ENABLE_WAIT_DIE_CC
      r = wait_die_acquire(s, p, s /* client id */, ctask->tid, 0 /* opid */, 
          e, t, ctx->ts, &l);
#else
      r = no_wait_acquire(e, t);
#endif
    
      if (r == LOCK_SUCCESS) {
        ; // great we have the lock
      } else if (r == LOCK_WAIT) {
        /* we have to spin now until value is ready. But we also need to
         * service other requests
         */
#if !defined(ENABLE_WAIT_DIE_CC)
        assert(0);
#endif
        assert(l);
        while (!l->ready)
          task_yield(p, TASK_STATE_READY);

      } else {
        // busted
        assert(r == LOCK_ABORT);
        p->naborts_local++;
        return NULL;
      }

      break;

    default:
      assert(0);
      break;
#endif
  }

#if GATHER_STATS
  if (t == OPTYPE_LOOKUP)
    p->nlookups_local++;
  else
    p->nupdates_local++;
#endif

  return e;
}

void *txn_op(struct task *ctask, struct hash_table *hash_table, int s, 
    struct hash_op *op, int target)
{
  void *value = NULL;
  struct elem *e = NULL;
  struct txn_ctx *ctx = &ctask->txn_ctx;
  struct partition *p = &hash_table->partitions[s];
  struct partition *l_p = NULL;

#if SHARED_EVERYTHING
  int is_local = 1;
  l_p = p;
#elif SHARED_NOTHING
  int is_local = 1;
  l_p = &hash_table->partitions[target];
#else
  int is_local = (s == target);
  l_p = p;
#endif

  //ugly hack to make item table global in all cases
  if ((op->key & TID_MASK) == ITEM_TID) {
    l_p = hash_table->g_partition;
    is_local = 1;
  }

  dprint("srv(%d): issue %s %s op key %" PRIu64 "\n", 
    s, is_local ? "local":"remote", 
    op->optype == OPTYPE_LOOKUP ? "lookup":"update", op->key);

  // if this is us, just call local procedure
  if (is_local) {
    //assert(op->key >= s * l_p->nrecs && op->key < (s * l_p->nrecs + l_p->nrecs));
    
    assert (l_p);

    e = local_txn_op(ctask, s, ctx, l_p, op);

    // it is possible for a local txn to fail as someone else might have
    // acquired a lock before us
    if (e)
      value = e->value;

  } else {

#if defined(SHARED_EVERYTHING) || defined(SHARED_NOTHING)
    assert(0);
#endif

    // call the corresponding authority and block
    smp_hash_doall(ctask, hash_table, s, target, 1, &op, &value);

    // the remote server can fail to return us data if someone else has a lock
    if (value) {
      e = (struct elem *)value;
      value = e->value;
      assert(value);
    } else {
#if GATHER_STATS
      p->naborts_remote++;
#endif
    }
  }

#if SHARED_NOTHING
  /* in shared nothing case, aborts are impossible. So don't bother
   * making copies of data as we wont need it
   */
#else
  if (value) {
    struct op_ctx *octx = &ctx->op_ctx[ctx->nops];
    octx->optype = op->optype;
    octx->e = e;
#if SHARED_EVERYTHING
    octx->target = s;
#else
    octx->target = target;
#endif

    if (op->optype == OPTYPE_UPDATE) {
      //octx->old_value = memalign(CACHELINE, e->size);
      //struct mem_tuple *m = plmalloc_alloc(p, e->size);
      //octx->old_value = m;
      //memcpy(m->data, e->value, e->size);
      octx->old_value = plmalloc_alloc(p, e->size);
      memcpy(octx->old_value, e->value, e->size);
    } else {
      octx->old_value = NULL;
    }

    dprint("srv(%d): done %s %s op key %" PRIu64 " ctx-nops %d\n", 
        s, is_local ? "local":"remote", 
        op->optype == OPTYPE_LOOKUP ? "lookup":"update", op->key, ctx->nops);


    ctx->nops++;
  }
#endif

  return value;
}

void txn_start(struct hash_table *hash_table, int s, int status, 
    struct txn_ctx *ctx)
{
  ctx->nops = 0;
  
  // if previous status was commit, this is a new txn. assign new ts.
  if (status == TXN_COMMIT)
    ctx->ts = read_tsc();
}

void txn_finish(struct task *ctask, struct hash_table *hash_table, int s, 
    int status, int mode, short *opids)
{
  struct txn_ctx *ctx = &ctask->txn_ctx;
  struct partition *p = &hash_table->partitions[s];
  int nops = ctx->nops;
  int nrels;

  while (--nops >= 0) {
    struct op_ctx *octx = &ctx->op_ctx[nops];
    int t = octx->optype;

    if (!octx->e) {
      // only possible in batch mode or if benchmark is tpcc
      assert(mode == TXN_BATCH || g_benchmark == &tpcc_bench);
      continue;
    }

    // lookups on item table do not need anything
    if ((octx->e->key & TID_MASK) == ITEM_TID) {
      assert(octx->optype == OPTYPE_LOOKUP);
      continue;
    }

    switch (t) {
      case OPTYPE_LOOKUP:
        assert (octx->old_value == NULL);

        // release element
        if (octx->target == s) {
#if SHARED_EVERYTHING
          selock_release(p, octx->e);
#else
#if ENABLE_WAIT_DIE_CC
          wait_die_release(s, p, s, ctask->tid, 
              opids ? opids[nops]: 0, octx->e);
#else
          no_wait_release(p, octx->e);
#endif //ENABLE_WAIT_DIE_cc
#endif //SHARED_EVERYTHING
        } else {
         mp_release_value(hash_table, s, octx->target, ctask->tid, 
              opids ? opids[nops] : 0, octx->e);
        }

        break;

      case OPTYPE_UPDATE:
        // should never get updates to item table
        assert((octx->e->key & TID_MASK) != ITEM_TID);

        assert(octx->old_value);

        size_t size = octx->e->size;

        // if this is a single txn that must be aborted, rollback
        if (status == TXN_ABORT) {
          //struct mem_tuple *m = octx->old_value;
          //memcpy(octx->e->value, m->data, size);
          memcpy(octx->e->value, octx->old_value, size);
        }

        plmalloc_free(p, octx->old_value, size);

        if (octx->target == s) {
#if SHARED_EVERYTHING
          selock_release(p, octx->e);
#else
#if ENABLE_WAIT_DIE_CC
          wait_die_release(s, p, s, ctask->tid, 
              opids ? opids[nops] : 0, octx->e);
#else
          no_wait_release(p, octx->e);
#endif //ENABLE_WAIT_DIE_CC
#endif //SHARED_EVERYTHING
        } else {
          mp_mark_ready(hash_table, s, octx->target, ctask->tid, 
              opids ? opids[nops] : 0, octx->e);
        }

        break;

      case OPTYPE_INSERT:
        // should never get inserts to item table
        assert((octx->e->key & TID_MASK) != ITEM_TID);

        // only single mode supported now
        assert(mode == TXN_SINGLE);

        if (octx->target == s) {
#if SHARED_EVERYTHING
          selock_release(p, octx->e);
#else
#if ENABLE_WAIT_DIE_CC
          wait_die_release(s, p, s, ctask->tid, opids ? opids[nops] : 0, octx->e);
#else
          no_wait_release(p, octx->e);
#endif //ENABLE_WAIT_DIE_CC
#endif //SHARED_EVERYTHING

          /* selock or wait_die/no_wiat will reset ref count once
           * if we are aborting, we reset it again. This will delete
           * the element and free up space
           */
          if (status == TXN_ABORT) {
            assert(octx->e->ref_count == 1);
            mp_release_value_(p, octx->e);            
          }

        } else {
          // XXX: If we need to abort a remote insert, we need a new 
          // HASHOP_DELETE that we don't support yet.
          // check if we need to do this
          assert(0);
          //mp_mark_ready(hash_table, s, octx->target, ctask->tid, 0, octx->e);
        }

        break;
    }
  }

  smp_flush_all(hash_table, s);

  ctx->nops = 0;
}

void txn_abort(struct task *ctask, struct hash_table *hash_table, int s, int mode)
{
#if SHARED_NOTHING
  assert(0);
#endif
  return txn_finish(ctask, hash_table, s, TXN_ABORT, mode, NULL);
}

void txn_commit(struct task *ctask, struct hash_table *hash_table, int s, int mode)
{
#if SHARED_NOTHING
  assert(0);
#endif

  return txn_finish(ctask, hash_table, s, TXN_COMMIT, mode, NULL);
}

int run_batch_txn(struct hash_table *hash_table, int s, void *arg, 
    struct task *ctask, int status)
{
  struct hash_query *query = (struct hash_query *)arg;
  struct partition *p = &hash_table->partitions[s];
  struct txn_ctx *ctx = &ctask->txn_ctx;
  int i, r = TXN_COMMIT;
  int nops = query->nops;
  int nremote = 0;
  int server = -1;
  //void **values = (void **) memalign(CACHELINE, sizeof(void *) * nops);
  //assert(values);

  void *values[MAX_OPS_PER_QUERY];
  short opids[MAX_OPS_PER_QUERY];
  short nopids = 0;

  // batch txns for micro benchmark
  assert(g_benchmark == &micro_bench);

  txn_start(hash_table, s, status, ctx);

  /* XXX: REWRITE THIS TO GATHER ALL REMOTE OPS AND SEND IT USING
   * SMP_HASH_DO_ALL 
   */
  // do all local first. if any local fails, no point sending remote requests
  for (i = 0; i < nops; i++) {
    struct hash_op *op = &query->ops[i];    
    server = op->key / hash_table->nrecs;

    // if local, get it
    if (server == s) {
      values[i] = txn_op(ctask, hash_table, s, &query->ops[i], server);
      opids[nopids++] = 0;

      if (!values[i]) {
        r = TXN_ABORT;
        goto final;
      }
    }
  }

  // now send all remote requests
  for (i = 0; i < nops; i++) {
    struct hash_op *op = &query->ops[i];    
    server = op->key / hash_table->nrecs;

    if (server != s) {
      if (op->optype == OPTYPE_LOOKUP) {
        smp_hash_lookup(ctask, hash_table, s, server, op->key, i);
        p->nlookups_remote++;
      } else {
        smp_hash_update(ctask, hash_table, s, server, op->key, i);
        p->nupdates_remote++;
      }

      values[i] = NULL;

      nremote++;
    }
  }

  /* we can have a max of 16 simultaneous ops from a single task as the opid
   * field is only 4 bits wide
   */
  assert(nremote < 16);

  ctask->npending = nremote;
  ctask->nresponses = 0;

  // now get all remote values
  if (nremote) {
    smp_flush_all(hash_table, s);

    task_yield(&hash_table->partitions[s], TASK_STATE_WAITING);

    // when we are scheduled again, we will have all data
    assert(nremote == ctask->nresponses);

    // match data to actual operation now
    for (i = 0; i < nremote; i++) {
      uint64_t val = ctask->received_responses[i];
      assert(HASHOP_GET_TID(val) == ctask->tid);

      short opid = HASHOP_GET_OPID(val);
      assert(opid < nops);

      assert(values[opid] == NULL);

      struct hash_op *op = &query->ops[opid];    
      struct op_ctx *octx = &ctx->op_ctx[ctx->nops];
      octx->optype = op->optype;
      octx->e = (struct elem *)HASHOP_GET_VAL(val);
      octx->target = -1;
      octx->old_value = NULL;
      ctx->nops++;
      opids[nopids++] = opid;

       if (octx->e) {
        int esize = octx->e->size;
        values[opid] = octx->e->value;

        if (op->optype == OPTYPE_UPDATE) {
          octx->old_value = plmalloc_alloc(p, esize);
          memcpy(octx->old_value, values[opid], esize);
        } else {
          octx->old_value = NULL;
        }
      } else {
        values[opid] = NULL;
        r = TXN_ABORT;
#if GATHER_STATS
        p->naborts_remote++;
#endif
      }
    }
  }

  assert(ctx->nops == nopids);

  // if some remote request failed, just abort
  if (r == TXN_ABORT)
    goto final;

  // now we have all values. Verify them.
  for (i = 0; i < query->nops; i++) {
    // in both lookup and update, we just check the value
    uint64_t *int_val = (uint64_t *)values[i];

    for (int j = 0; j < YCSB_NFIELDS; j++) {
      assert (int_val[j] == query->ops[i].key);
    }
  }

final:
  if (r == TXN_COMMIT)
    txn_finish(ctask, hash_table, s, TXN_COMMIT, TXN_BATCH, opids);
  else
    txn_finish(ctask, hash_table, s, TXN_ABORT, TXN_BATCH, opids);

  //free(values);

  return r;
}

void process_releases(struct hash_table *hash_table, int s)
{
  struct box_array *boxes = hash_table->boxes;
  uint64_t inbuf[ONEWAY_BUFFER_SIZE];
  struct partition *p = &hash_table->partitions[s];
  int nservers = hash_table->nservers;
  int s_coreid = hash_table->thread_data[s].core;
  char skip_list[BITNSLOTS(MAX_SERVERS)];

  memset(skip_list, 0, sizeof(skip_list));

#if ENABLE_SOCKET_LOCAL_TXN
  for (int i = 0; i < nservers; i++) {
      int t_coreid = hash_table->thread_data[i].core;

      // check only cores in our own socket of leader cores in other sockets
      if (s_coreid % 4 == t_coreid % 4 || t_coreid < 4) {
        ;
      } else {
        BITSET(skip_list, i);
      }
  }
#endif

  for (int i = 0; i < nservers; i++) {
    if (i == s)
      continue;

    if (BITTEST(skip_list, i)) {
      continue;
    }

    struct onewaybuffer *b = &boxes[i].boxes[s].in; 
    int count = b->wr_index - b->rd_index;
    if (count == 0) 
      continue;

    count = buffer_peek(b, ONEWAY_BUFFER_SIZE, inbuf);
    assert(count);

    dprint("srv(%d): read %d messages from client %d\n", s, count, i);

    // get all elems
    int j = 0;
    while (j < count) {
      uint64_t optype = inbuf[j] & HASHOP_MASK;

      if (optype != HASHOP_RELEASE)
        break;

      struct elem *t = (struct elem *)(HASHOP_GET_VAL(inbuf[j]));
      short tid = HASHOP_GET_TID(inbuf[j]);
      short opid = HASHOP_GET_OPID(inbuf[j]);

      dprint("srv(%d): cl %d before release %" PRIu64 " rc %" PRIu64 "\n", 
          s, i, t->key, t->ref_count);

#if ENABLE_WAIT_DIE_CC
      wait_die_release(s, p, i, tid, opid, t);
#else
      no_wait_release(p, t);
#endif

      buffer_seek(b, RELEASE_MSG_LENGTH);
      j += RELEASE_MSG_LENGTH;
    }
  }
}
void process_requests(struct hash_table *hash_table, int s)
{
  struct box_array *boxes = hash_table->boxes;
  uint64_t inbuf[ONEWAY_BUFFER_SIZE];
  struct partition *p = &hash_table->partitions[s];
  int nservers = hash_table->nservers;
  int s_coreid = hash_table->thread_data[s].core;
  char skip_list[BITNSLOTS(MAX_SERVERS)];

  struct req {
    struct elem *e;
    uint64_t optype;
    int tid;
    int opid;
    uint64_t ts;
    int r;
  } reqs[ONEWAY_BUFFER_SIZE];

  int nreqs;

  memset(skip_list, 0, sizeof(skip_list));

#if ENABLE_SOCKET_LOCAL_TXN
  /* Having cores check msg buffer incurs overhead in the cross socket case
   * as cache coherence enforcement causes snooping. So to minimize the 
   * overhead, we have a mode where we do only socket local txns. This can
   * be easily enforced at runtime by using a scheduler that queues a txn
   * in same socket as thread that has the data.
   *
   * XXX: This requires determining for a given core, all others in socket.
   * This can be determined programatically. For now, we hack it in and 
   * make it specific to diascld33 -- the 4 socket server where mapping is
   * 0,4,8,...72: s0
   * 1,5,9,...73: s1
   * 2,6,10,...74: s3
   * 3,7,11,...75: s4
   *
   * s will range from 0 to nservers. Given s, s/18 is socketid
   * on which this server sits. Then, based on above mapping min clientid is 
   * socketid * 18 the max clientid in that socket is min + 18.
   *
   */
  
  /* check only socket-local guys plus first core on other sockets
   * Enforce this by setting appropriate bits in the skip list
   */
  for (int i = 0; i < nservers; i++) {
      int t_coreid = hash_table->thread_data[i].core;

      // check only cores in our own socket of leader cores in other sockets
      if (s_coreid % 4 == t_coreid % 4 || t_coreid < 4) {
        ;
      } else {
        BITSET(skip_list, i);
      }
  }
#endif

#if 0
  process_releases(hash_table, s);
#endif

  for (int i = 0; i < nservers; i++) {
    if (i == s)
      continue;

    if (BITTEST(skip_list, i)) {
      continue;
    }

    struct onewaybuffer *b = &boxes[i].boxes[s].in; 
    int count = b->wr_index - b->rd_index;
    if (count == 0) 
      continue;

    /* We can get many requests from one server as there can be many tasks.
     * We process it as follows
     * 1) get all requests and retrieve all elements
     * 2) group elements into sets, one set per unique task
     * 3) process task groups one at a time.
     *
     * Within task group, there are 3 cases for each request depending on CC:
     * 1) abort 2) success 3) wait 
     * 1) Even if 1 request aborts, whole txn will abort. So no point in sending
     * back any other request. So just fail all requests
     * 2) if success, send back data to caller
     * 3) if wait, don't do anything. data will be sent back later
     */
    count = buffer_read_all(b, ONEWAY_BUFFER_SIZE, inbuf, 0);
    assert(count);

    dprint("srv(%d): read %d messages from client %d\n", s, count, i);

    // get all elems
    int j = 0;
    nreqs = 0;
    while (j < count) {
      uint64_t optype = inbuf[j] & HASHOP_MASK;

      switch(optype) {
        case HASHOP_RELEASE:
        {
          struct elem *t = (struct elem *)(HASHOP_GET_VAL(inbuf[j]));
          short tid = HASHOP_GET_TID(inbuf[j]);
          short opid = HASHOP_GET_OPID(inbuf[j]);

          dprint("srv(%d): cl %d before release %" PRIu64 " rc %" PRIu64 "\n", 
              s, i, t->key, t->ref_count);

#if ENABLE_WAIT_DIE_CC
          wait_die_release(s, p, i, tid, opid, t);
#else
          no_wait_release(p, t);
#endif

          j += RELEASE_MSG_LENGTH;
          break;
        }
        case HASHOP_INSERT:
        {
          hash_key key = HASHOP_GET_VAL(inbuf[j]);
          short tid = HASHOP_GET_TID(inbuf[j]);
          short opid = HASHOP_GET_OPID(inbuf[j]);
          size_t sz = inbuf[j + 1];

          dprint("srv(%d): cl %d inserting key %"PRIu64" sz %d\n",
              s, i, key, sz);

          struct elem *e = hash_insert(p, key, sz, NULL);
          assert(e);

          e->ref_count = 1;
          p->ninserts++;

          int r;
#if ENABLE_WAIT_DIE_CC
          struct lock_entry *l;

          r = wait_die_acquire(s, p, i, tid, opid, e, OPTYPE_INSERT, 
              inbuf[j + 2], &l);
#else
          r = no_wait_acquire(e, OPTYPE_INSERT);
#endif

          // insert can only succeed
          assert (r == LOCK_SUCCESS);

          // reply back
          uint64_t out_msg = MAKE_HASH_MSG(tid, opid, (unsigned long)e, 0);
          buffer_write_all(&boxes[i].boxes[s].out, 1, &out_msg, 0);
     
          j += INSERT_MSG_LENGTH;

          break;
        }
        case HASHOP_LOOKUP:
        case HASHOP_UPDATE:
        {
          hash_key key = HASHOP_GET_VAL(inbuf[j]);
          struct req *req = &reqs[nreqs];
          req->optype = optype;
          req->tid = HASHOP_GET_TID(inbuf[j]);
          req->opid = HASHOP_GET_OPID(inbuf[j]);
          req->r = LOCK_INVALID;
#if ENABLE_WAIT_DIE_CC
          req->ts = inbuf[j + 1];
#endif

          dprint("srv (%d): cl %d tid %d opid %d %s %" PRIu64 "\n", s, i, 
              req->tid, req->opid, 
              OPTYPE_STR(optype), key);

          req->e = hash_lookup(p, key);
          if (!req->e) {
            dprint("srv (%d): cl %d %s %" PRIu64 " failed\n", s, i, 
                OPTYPE_STR(optype), key);
          }
          assert(req->e);

          nreqs++;
          j += LOOKUP_MSG_LENGTH;
          break;
        }
        default:
        {
          printf("cl %d invalid message type %lx\n", i, optype); 
          fflush(stdout);
          assert(0);
          break;
        }
      }
    }

    // do trial task at a time
    for (j = 0; j < nreqs; j++) {
      struct req *req = &reqs[j];

      assert(req->optype != HASHOP_RELEASE);
      assert(req->optype == HASHOP_LOOKUP || req->optype == HASHOP_UPDATE);
 
      // if someone marked this as abort, don't do req
      if (req->r != LOCK_INVALID) {
        continue;
      }

#if ENABLE_WAIT_DIE_CC
      req->r = wait_die_check_acquire(s, p, i, req->tid, req->opid, req->e,
          req->optype == HASHOP_LOOKUP ? OPTYPE_LOOKUP : OPTYPE_UPDATE, 
          req->ts);
#else
      req->r = no_wait_check_acquire(req->e,
          req->optype == HASHOP_LOOKUP ? OPTYPE_LOOKUP : OPTYPE_UPDATE);
#endif

      char abort = 0;

      if (req->r == LOCK_ABORT) {
        abort = 1;
        dprint("srv (%d): cl %d tid %d opid %d %s %" PRIu64 " aborted\n", 
            s, i, req->tid, req->opid, 
            req->optype == HASHOP_LOOKUP ? "lookup" : "update", req->e->key);
      }

      /* now try acquring locks of all other requests from same task
       * in case j aborted, set all others to abort as well
       */
      for (int k = j + 1; k < nreqs; k++) {
        if (req->tid == reqs[k].tid) {
          assert(req->e != reqs[k].e);

          if (req->r == LOCK_ABORT) {
            reqs[k].r = LOCK_ABORT;
          } else {
            assert (req->r != LOCK_INVALID);

#if ENABLE_WAIT_DIE_CC
            reqs[k].r = wait_die_check_acquire(s, p, i, reqs[k].tid, 
                reqs[k].opid, reqs[k].e, 
                reqs[k].optype == HASHOP_LOOKUP ? OPTYPE_LOOKUP : OPTYPE_UPDATE, 
                reqs[k].ts);
#else
            reqs[k].r = no_wait_check_acquire(reqs[k].e,
                reqs[k].optype == HASHOP_LOOKUP ? OPTYPE_LOOKUP : OPTYPE_UPDATE);
#endif

            if (reqs[k].r == LOCK_ABORT) {
              req->r = LOCK_ABORT;
              abort = 1;
            }
          }
        }
      }

      if (!abort) {
        //actually acquire the locks for all requests from j's task
        for (int k = j; k < nreqs; k++) {
          if (reqs[j].tid != reqs[k].tid)
            continue;

          assert(reqs[k].r != LOCK_INVALID);

#if ENABLE_WAIT_DIE_CC
          struct lock_entry *l = NULL;

          int res = wait_die_acquire(s, p, i, reqs[k].tid, 
              reqs[k].opid, reqs[k].e, 
              reqs[k].optype == HASHOP_LOOKUP ? OPTYPE_LOOKUP : OPTYPE_UPDATE, 
              reqs[k].ts, &l);

#else
          int res = no_wait_acquire(reqs[k].e, reqs[k].optype == HASHOP_LOOKUP ? 
              OPTYPE_LOOKUP : OPTYPE_UPDATE);
#endif
          assert(res == reqs[k].r);
        }

      } else {
        assert(req->r == LOCK_ABORT);

        // if any of k failed, fail all of them
        for (int k = j + 1; k < nreqs; k++) {
          if (req->tid == reqs[k].tid)
            req[k].r = LOCK_ABORT;
        }
      }
    }

    // now send back the responses
    for (j = 0; j < nreqs; j++) {
      uint64_t out_msg;
      struct req *req = &reqs[j];
      int r = req->r;

      // skip all release messages
      assert (req->optype != HASHOP_RELEASE);

      assert(r != LOCK_INVALID);

      /* now, skip waits, respond back for success and aborts and get the locks
       * for real
       */
      if (r == LOCK_ABORT) {
        out_msg = MAKE_HASH_MSG(req->tid, req->opid, 0, 0);
        buffer_write_all(&boxes[i].boxes[s].out, 1, &out_msg, 0);
      } else if (r == LOCK_SUCCESS) {
        assert(req->e->ref_count > 1);
        out_msg = MAKE_HASH_MSG(req->tid, req->opid, (unsigned long)req->e, 0);
        buffer_write_all(&boxes[i].boxes[s].out, 1, &out_msg, 0);
      } else {
        assert (r == LOCK_WAIT);
      }
    }

    buffer_flush(&boxes[i].boxes[s].out);
  }
}

void *hash_table_server(void* args)
{
  int i, r;
  const int s = ((struct thread_args *) args)->id;
  const int c = ((struct thread_args *) args)->core;
  struct hash_table *hash_table = ((struct thread_args *) args)->hash_table;
  struct partition *p = &hash_table->partitions[s];
  void *query;
  __attribute__((unused)) int pct = 10;

  set_affinity(c);
   
  double tstart = now();

#if SHARED_EVERYTHING
  /* load only one partition in case of shared everything */
  if (s == 0)
    g_benchmark->load_data(hash_table, s);
#else
  /* always load for trireme */
  g_benchmark->load_data(hash_table, s);
#endif

  double tend = now();

  printf("srv %d load time %.3f\n", s, tend - tstart);
  printf("srv %d rec count: %d partition sz %lu-KB "
      "tx count: %d, per_txn_op cnt: %d\n", s, p->ninserts, p->size / 1024, 
      niters, ops_per_txn);

  //double avg;
  //double stddev;
  //stats_get_buckets(hash_table, s, &avg, &stddev);
  //printf("srv %d hash table occupancy avg %0.3f stddev %0.3f\n", s, avg, stddev);
  //fflush(stdout);

  pthread_mutex_lock(&hash_table->create_client_lock); 

  nready++;

  pthread_mutex_unlock(&hash_table->create_client_lock); 

  while (nready != hash_table->nservers) ;

  printf("srv %d starting txns\n", s);
  fflush(stdout);

  query = g_benchmark->alloc_query();
  assert(query);

  tstart = now();

  task_libinit(s);

#if 0
  for (i = 0; i < niters; i++) {
    // run query as txn
    g_benchmark->get_next_query(hash_table, s, query);

    r = TXN_COMMIT;

    do {
      if (batch_size == 1)
        r = g_benchmark->run_txn(hash_table, s, query, r);
      else
        r = run_batch_txn(hash_table, s, query);

      // see if we need to answer someone
#if !defined (SHARED_EVERYTHING) && !defined (SHARED_NOTHING)
      process_requests(hash_table, s);
#endif

      if (r == TXN_ABORT) {
        dprint("srv(%d): rerunning aborted txn %d\n", s, i);
      }

    } while (r == TXN_ABORT);

#if PRINT_PROGRESS
    if (i % 100000 == 0)
    //if (i % 1000 == 0)
      printf("srv(%d): completed txn %d\n", s, i);
#endif

  }
#endif

  tend = now();

  printf("srv %d query time %.3f\n", s, tend - tstart);
  fflush(stdout);

  p->tps = niters / (tend - tstart);

  pthread_mutex_lock(&hash_table->create_client_lock); 

  nready--;

  pthread_mutex_unlock(&hash_table->create_client_lock); 

  while (nready != 0)
#if !defined (SHARED_EVERYTHING) && !defined (SHARED_NOTHING)
    process_requests(hash_table, s);
#else
    ;
#endif

  printf("srv %d quitting \n", s);
  fflush(stdout);

  if (g_benchmark->verify_txn)
    g_benchmark->verify_txn(hash_table, s);

  // destory out dses
#if !defined (SHARED_EVERYTHING)
  /* in shared nothing and trireme case, we can safely delete
   * the partition now as no one else will be directly accessing
   * data from the partition. In shared everything case, some other
   * thread might still be doing verification. So we don't destroy now.
   */
  destroy_hash_partition(p);
#endif

 #if 0
  // stay running until someone else needs us
  while (quitting == 0) {
    quitting = hash_table->quitting;
    process_requests(hash_table, s);
  }
#endif

  return NULL;
}

int smp_hash_lookup(struct task *ctask, struct hash_table *hash_table, 
    int client_id, int server, hash_key key, short op_id)
{
  uint64_t msg_data[2];

  assert(server >= 0 && server < hash_table->nservers);

  dprint("srv(%d): sending lookup for key %"PRIu64" to srv %d\n", client_id, key, server);

  msg_data[0] = MAKE_HASH_MSG(ctask->tid, op_id, key, HASHOP_LOOKUP);

#if ENABLE_WAIT_DIE_CC
        // send timestamp as a payload in case of wait die cc
  msg_data[1] = ctask->txn_ctx.ts;
  buffer_write_all(&hash_table->boxes[client_id].boxes[server].in, 2, msg_data, 0);
#else
  buffer_write_all(&hash_table->boxes[client_id].boxes[server].in, 1, msg_data, 0);
#endif

  return 1;
}

#if 0
int smp_hash_insert(struct hash_table *hash_table, int client_id, hash_key key, int size)
{
  uint64_t msg_data[INSERT_MSG_LENGTH];
  int s = g_benchmark->hash_get_server(hash_table, key);

  msg_data[0] = (unsigned long)key | HASHOP_INSERT;
  msg_data[1] = (unsigned long)size;

  dprint("srv(%d): sending insert for key %"PRIu64" to srv %d\n", client_id, key, s);

  buffer_write_all(&hash_table->boxes[client_id].boxes[s].in, INSERT_MSG_LENGTH, msg_data, 0);

  return 1;
}
#endif

int smp_hash_update(struct task *ctask, struct hash_table *hash_table, 
    int client_id, int server, hash_key key, short op_id)
{
  uint64_t msg_data[2];

  assert(server >= 0 && server < hash_table->nservers);

  dprint("srv(%d): sending update for key %"PRIu64" to srv %d\n", client_id, 
      key, server);

  msg_data[0] = MAKE_HASH_MSG(ctask->tid, op_id, key, HASHOP_UPDATE);

#if ENABLE_WAIT_DIE_CC
        // send timestamp as a payload in case of wait die cc
  msg_data[1] = ctask->txn_ctx.ts;
  buffer_write_all(&hash_table->boxes[client_id].boxes[server].in, 2, msg_data, 0);
#else
  buffer_write_all(&hash_table->boxes[client_id].boxes[server].in, 1, msg_data, 0);
#endif

  return 1;
}

void smp_hash_doall(struct task *ctask, struct hash_table *hash_table, 
    int client_id, int s, int nqueries, struct hash_op **queries, 
    void **values)
{
  int r, i;
  uint64_t val;

  struct box_array *boxes = hash_table->boxes;
  uint64_t msg_data[3];

  for(int i = 0; i < nqueries; i++) {

    assert(i == 0);

    switch (queries[i]->optype) {
      case OPTYPE_LOOKUP:
        //s = g_benchmark->hash_get_server(hash_table, queries[i]->key);

        dprint("srv(%d): issue remote lookup op key %" PRIu64 " to %d\n", 
          client_id, queries[i]->key, s);

        int s_coreid = hash_table->thread_data[s].core;
        int c_coreid = hash_table->thread_data[client_id].core;

        msg_data[0] = MAKE_HASH_MSG(ctask->tid, i, queries[i]->key, 
            HASHOP_LOOKUP);

#if ENABLE_WAIT_DIE_CC
        // send timestamp as a payload in case of wait die cc
        msg_data[1] = ctask->txn_ctx.ts;
        buffer_write_all(&boxes[client_id].boxes[s].in, 2, msg_data, 0);
#else
        buffer_write_all(&boxes[client_id].boxes[s].in, 1, msg_data, 0);
#endif

#if GATHER_STATS
        hash_table->partitions[client_id].nlookups_remote++;
#endif
        break;
      case OPTYPE_UPDATE:
        //s = g_benchmark->hash_get_server(hash_table, queries[i]->key);

        dprint("srv(%d): issue remote update op key %" PRIu64 " to %d\n", 
          client_id, queries[i]->key, s);

        msg_data[0] = MAKE_HASH_MSG(ctask->tid, i, queries[i]->key, 
            HASHOP_UPDATE);

#if ENABLE_WAIT_DIE_CC
        msg_data[1] = ctask->txn_ctx.ts;
        buffer_write_all(&boxes[client_id].boxes[s].in, 2, msg_data, 0);
#else
        buffer_write_all(&boxes[client_id].boxes[s].in, 1, msg_data, 0);
#endif

#if GATHER_STATS
        hash_table->partitions[client_id].nupdates_remote++;
#endif
        break;

      case OPTYPE_INSERT:
        //s = g_benchmark->hash_get_server(hash_table, queries[i]->key);

        dprint("srv(%d): issue remote insert op key %" PRIu64 " to %d\n", 
          client_id, queries[i]->key, s);

        msg_data[0] = MAKE_HASH_MSG(ctask->tid, i, queries[i]->key, 
            HASHOP_INSERT);

        msg_data[1] = queries[i]->size;

#if ENABLE_WAIT_DIE_CC
        msg_data[2] = ctask->txn_ctx.ts;
        buffer_write_all(&boxes[client_id].boxes[s].in, 3, msg_data, 0);
#else
        buffer_write_all(&boxes[client_id].boxes[s].in, 2, msg_data, 0);
#endif

#if GATHER_STATS
        hash_table->partitions[client_id].nupdates_remote++;
#endif
        break;

      default:
        assert(0);
        break;
    }
  }

  ctask->npending = nqueries;
  ctask->nresponses = 0;

  // after queueing all the queries we flush all buffers and read all remaining values
  smp_flush_all(hash_table, client_id);

  task_yield(&hash_table->partitions[client_id], TASK_STATE_WAITING);

  // we should have data when we return
  assert(ctask->npending == ctask->nresponses);

  i = 0;
  while (i < nqueries) {
    int ps;

    if (queries[i]->optype == OPTYPE_PLOCK_ACQUIRE) {
      ps = queries[i]->key;
    } else {
      //ps = g_benchmark->hash_get_server(hash_table, queries[i]->key);
      ps = s;
    }

    val = ctask->received_responses[i];

    assert(HASHOP_GET_TID(val) == ctask->tid);
    assert(HASHOP_GET_OPID(val) == i);

    values[i] = (void *)(unsigned long)HASHOP_GET_VAL(val);
    i++;

    //process_requests(hash_table, client_id);
  }
}

void smp_flush_all(struct hash_table *hash_table, int client_id)
{
  for (int i = 0; i < hash_table->nservers; i++) {
    /* printf("srv(%d): flushing buffer %d rd count %d wcount %d twcount %d\n", 
        client_id, i, hash_table->boxes[client_id].boxes[i].in.rd_index, 
        hash_table->boxes[client_id].boxes[i].in.wr_index,
        hash_table->boxes[client_id].boxes[i].in.tmp_wr_index);
    */

    buffer_flush(&hash_table->boxes[client_id].boxes[i].in);
  }
}

/**
 * Value Memory Management Operations
 */
int is_value_ready(struct elem *e)
{
  return (e->ref_count & DATA_READY_MASK) == 0 ? 1 : 0;
}

void mp_release_value_(struct partition *p, struct elem *e)
{
  e->ref_count = (e->ref_count & (~DATA_READY_MASK)) - 1;

  dprint("srv(%ld): Releasing key %" PRIu64 " rc %" PRIu64 "\n", 
      p - hash_table->partitions, e->key,
      e->ref_count);

  if (e->ref_count == 0) {
    //printf("key %" PRIu64 " 0 rc\n", e->key);
    assert(g_benchmark != &micro_bench);
    hash_remove(p, e);
  }
}

void mp_send_release_msg_(struct hash_table *hash_table, int client_id, 
    int s, int task_id, int op_id, void *ptr, int force_flush)
{
  struct elem *e = (struct elem *)ptr;

#if defined(SHARED_EVERYTHING) || defined(SHARED_NOTHING)
  assert(0);
#endif

  assert (s < hash_table->nservers && client_id < hash_table->nservers);

  // XXX: we are exploiting the 48-bit address here. 
  uint64_t msg_data = MAKE_HASH_MSG(task_id, op_id, (uint64_t)e, HASHOP_RELEASE);
  assert(((struct elem *)HASHOP_GET_VAL(msg_data)) == e);

  dprint("srv(%ld): sending release msg %"PRIu64" for key %" PRIu64 
      " rc %" PRIu64 " to %d\n", client_id, msg_data, e->key, e->ref_count, s);

  buffer_write_all(&hash_table->boxes[client_id].boxes[s].in, 1, &msg_data, 
          force_flush);

  /* printf("srv(%d): buffer %d post release rd count %d wcount %d twcount %d\n", 
      client_id, s, hash_table->boxes[client_id].boxes[s].in.rd_index, 
      hash_table->boxes[client_id].boxes[s].in.wr_index,
      hash_table->boxes[client_id].boxes[s].in.tmp_wr_index);
      */

}

void mp_release_value(struct hash_table *hash_table, int client_id, int target,
    int task_id, int op_id, void *ptr)
{
  mp_send_release_msg_(hash_table, client_id, target, task_id, op_id, ptr, 
      0 /* force_flush */);
}

void mp_mark_ready(struct hash_table *hash_table, int client_id, int target,
    int task_id, int op_id, void *ptr)
{
  dprint("srv(%d): sending release msg key %" PRIu64 " rc %" PRIu64 " \n", 
      client_id, ((struct elem *)ptr)->key, ((struct elem*)ptr)->ref_count);

  mp_send_release_msg_(hash_table, client_id, target, task_id, op_id, ptr, 
      0 /* force_flush */);
}

void mp_release_plock(int s, int c)
{
  uint64_t msg_data = HASHOP_PLOCK_RELEASE;

  buffer_write_all(&hash_table->boxes[s].boxes[c].in, 1, &msg_data, 1);
}

void mp_send_reply(int s, int c, short task_id, short op_id, struct elem *e)
{
  if (s == c)
    return;

  uint64_t msg_data = MAKE_HASH_MSG(task_id, op_id, (unsigned long)e, 0);

  buffer_write_all(&hash_table->boxes[c].boxes[s].out, 1, &msg_data, 1);
}

/**
 * Hash Table Counters and Stats
 */
void stats_reset(struct hash_table *hash_table)
{
  for (int i = 0; i < hash_table->nservers; i++) {
    hash_table->partitions[i].ninserts = 0;
    hash_table->partitions[i].nlookups_local = 0;
    hash_table->partitions[i].nupdates_local = 0;
    hash_table->partitions[i].naborts_local = 0;
    hash_table->partitions[i].nlookups_remote = 0;
    hash_table->partitions[i].nupdates_remote = 0;
    hash_table->partitions[i].naborts_remote = 0;
  }
}

int stats_get_ncommits(struct hash_table *hash_table)
{
  int ncommits = 0;
  for (int i = 0; i < hash_table->nservers; i++) {
    printf("srv %d commits %d\n", i, 
      hash_table->partitions[i].ncommits);

    ncommits += hash_table->partitions[i].ncommits;
  }

  printf("total commits %d\n", ncommits);

  return ncommits;

}

int stats_get_nlookups(struct hash_table *hash_table)
{
  int nlookups = 0;
  for (int i = 0; i < hash_table->nservers; i++) {
    printf("srv %d lookups local: %d remote %d\n", i, 
      hash_table->partitions[i].nlookups_local,
      hash_table->partitions[i].nlookups_remote);

    nlookups += hash_table->partitions[i].nlookups_local + 
      hash_table->partitions[i].nlookups_remote;
  }

  printf("total lookups %d\n", nlookups);

  return nlookups;
}

int stats_get_nupdates(struct hash_table *hash_table)
{
  int nupdates = 0;
  for (int i = 0; i < hash_table->nservers; i++) {
    printf("srv %d updates local: %d remote %d \n", i, 
      hash_table->partitions[i].nupdates_local,
      hash_table->partitions[i].nupdates_remote);

    nupdates += hash_table->partitions[i].nupdates_local + 
      hash_table->partitions[i].nupdates_remote;
  }

  printf("total updates %d\n", nupdates);

  return nupdates;
}

int stats_get_naborts(struct hash_table *hash_table)
{
  int naborts = 0;
  for (int i = 0; i < hash_table->nservers; i++) {
    printf("srv %d aborts local: %d remote %d \n", i, 
      hash_table->partitions[i].naborts_local,
      hash_table->partitions[i].naborts_remote);

    naborts += hash_table->partitions[i].naborts_local + 
      hash_table->partitions[i].naborts_remote;
  }

  return naborts;
}


int stats_get_ninserts(struct hash_table *hash_table)
{
  int ninserts = 0;
  for (int i = 0; i < hash_table->nservers; i++) {
    printf("Server %d : %d tuples %lu-KB mem \n", i,  
      hash_table->partitions[i].ninserts, hash_table->partitions[i].size / 1024);
    ninserts += hash_table->partitions[i].ninserts;
  }

  printf("total inserts %d\n", ninserts);

  return ninserts;
}

void stats_get_buckets(struct hash_table *hash_table, int server, double *avg, double *stddev)
{
  struct partition *p = &hash_table->partitions[server];

  int nelems = 0;
  struct elem *e;

  for (int i = 0; i < p->nhash; i++) {
    e = LIST_FIRST(&p->table[i].chain);
    while (e != NULL) {
      nelems++;
      e = LIST_NEXT(e, chain);
    }
  }
  *avg = (double)nelems / p->nhash;
  *stddev = 0;

  for (int i = 0; i < p->nhash; i++) {
    e = LIST_FIRST(&p->table[i].chain);
    int length = 0;
    while (e != NULL) {
      e = LIST_NEXT(e, chain);
      length++;
    }

    *stddev += (*avg - length) * (*avg - length);
  }

  *stddev = sqrt(*stddev / (nelems - 1));
}

void stats_get_mem(struct hash_table *hash_table, size_t *used, size_t *total)
{
  struct partition *p;
  size_t m = 0, u = 0;

  for (int i = 0; i < hash_table->nservers; i++) {
    p = &hash_table->partitions[i];
  
    m += p->nrecs;
    u += p->size;
  }

  *total = m;
  *used = u;
}

void stats_set_track_cpu_usage(struct hash_table *hash_table, int track_cpu_usage)
{
  hash_table->track_cpu_usage = track_cpu_usage;
}

double stats_get_cpu_usage(struct hash_table *hash_table)
{
  struct partition *p;
  uint64_t busy = 0;
  uint64_t idle = 0;

  for (int i = 0; i < hash_table->nservers; i++) {
    p = &hash_table->partitions[i];
    busy += p->busyclock;
    idle += p->idleclock;
  }

  return (double)busy / (double)(busy + idle);
}

double stats_get_tps(struct hash_table *hash_table)
{
  double tps = 0;

  for (int i = 0; i < hash_table->nservers; i++) {
    printf("Server %d: %0.9fM TPS\n", i,  
      (double)hash_table->partitions[i].tps / 1000000);

    tps += ((double)hash_table->partitions[i].tps / 1000000);
  }
  
  return tps;
}

