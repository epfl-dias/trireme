#include <sys/sysinfo.h>

#include "headers.h"
#include "onewaybuffer.h"
#include "partition.h"
#include "smphashtable.h"
#include "benchmark.h"
#include "tpcc.h"
#include "plmalloc.h"
#include "twopl.h"

/** 
 * Hash Table Operations
 */
#define HASHOP_MASK       0xF000000000000000
#define HASHOP_LOOKUP     0x1000000000000000 
#define HASHOP_INSERT     0x2000000000000000 
#define HASHOP_UPDATE     0x3000000000000000 
#define HASHOP_RELEASE    0x4000000000000000 

#define INSERT_MSG_LENGTH 2

#define MAX_PENDING_PER_SERVER  (ONEWAY_BUFFER_SIZE - (CACHELINE >> 3))

static volatile int nready = 0;

extern struct benchmark *g_benchmark;
extern double alpha;
extern int niters;
extern int batch_size;
extern int ops_per_txn, nhot_servers;
extern struct hash_table *hash_table;

// Forward declarations
void *hash_table_server(void* args);
int is_value_ready(struct elem *e);
void process_requests(struct hash_table *hash_table, int s);

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

int is_local_op(struct hash_table *hash_table, int s, hash_key key)
{
  return g_benchmark->hash_get_server(hash_table, key) == s;
}

struct elem *local_txn_op(struct txn_ctx *ctx, struct partition *p, 
    struct hash_op *op)
{
  struct elem *e;
  uint32_t t = op->optype;

  switch (t) {
    case OPTYPE_INSERT:
      // should not get a insert to item partition
      assert (p != hash_table->g_partition);

      e = hash_insert(p, op->key, op->size, NULL);
      e->ref_count = DATA_READY_MASK | 2; 
      p->ninserts++;
      break;

    case OPTYPE_LOOKUP:
      e = hash_lookup(p, op->key);
      assert(e);

#if SHARED_EVERYTHING
      if (!selock_acquire(p, e, OPTYPE_LOOKUP, ctx->ts)) {
        p->naborts_local++;
        return NULL;
      }
#elif SHARED_NOTHING
      /* Do absolutely nothing for shared nothing as it proceeds by first
       * getting partition locks. So there is no need to get record locks
       * as access to the whole partition itself is serialized
       */
#else

      // if value is not ready, lookup and updates fail
      if (!is_value_ready(e)) {
        assert(e->ref_count == (DATA_READY_MASK | 2));

        dprint("lookup %" PRIu64 " rc %" PRIu64 " aborted \n", op->key, 
            e->ref_count);

        p->naborts_local++;
        return NULL;
      }

      if (p != hash_table->g_partition)
   	    e->ref_count++;
#endif

#if GATHER_STATS
      p->nlookups_local++;
#endif
      break;

    case OPTYPE_UPDATE:
      // should not get a update to item partition
      assert (p != hash_table->g_partition);

      e = hash_lookup(p, op->key);
      if (!e) {
        printf("srv(%d): lookup %"PRIu64" failed\n", p - &hash_table->partitions[0], op->key);
        assert(e);
      }

#if SHARED_EVERYTHING
      if (!selock_acquire(p, e, OPTYPE_UPDATE, ctx->ts)) {
        p->naborts_local++;
        return NULL;
      }
#elif SHARED_NOTHING
      /* Do absolutely nothing for shared nothing as it proceeds by first
       * getting partition locks. So there is no need to get record locks
       * as access to the whole partition itself is serialized
       */
#else

      // if pending lookups are there, updates fail
      if (!is_value_ready(e) || e->ref_count > 1) {
        dprint("update %" PRIu64 " rc %" PRIu64 " aborted \n", op->key,
            e->ref_count);

        p->naborts_local++;

        return NULL;
      }

      e->ref_count = DATA_READY_MASK | 2; 
#endif

#if GATHER_STATS
      p->nupdates_local++;
#endif
      break;

    default:
      assert(0);
      break;
  }

  return e;
}

void *txn_op(struct hash_table *hash_table, int s, struct partition *l_p, 
    struct hash_op *op, int is_local)
{
  void *value = NULL;
  struct elem *e = NULL;
  struct partition *p = &hash_table->partitions[s];
  struct txn_ctx *ctx = &p->txn_ctx;
  //int is_local = is_local_op(hash_table, s, op->key);

  if (!l_p)
    l_p = p;

  dprint("srv(%d): issue %s %s op key %" PRIu64 "\n", 
    s, is_local ? "local":"remote", 
    op->optype == OPTYPE_LOOKUP ? "lookup":"update", op->key);

  // if this is us, just call local procedure
  if (is_local) {
    //assert(op->key >= s * l_p->nrecs && op->key < (s * l_p->nrecs + l_p->nrecs));
    
    e = local_txn_op(ctx, l_p, op);

    // it is possible for a local txn to fail as someone else might have
    // acquired a lock before us
    if (e)
      value = e->value;

  } else {

    // call the corresponding authority and block
    smp_hash_doall(hash_table, s, 1, &op, &value);

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
    octx->is_local = is_local;

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

    ctx->nops++;
  }
#endif

  return value;
}

void txn_start(struct hash_table *hash_table, int s, int status)
{
  struct txn_ctx *ctx = &hash_table->partitions[s].txn_ctx;

  ctx->nops = 0;
  
  // if previous status was commit, this is a new txn. assign new ts.
  if (status == TXN_COMMIT)
    ctx->ts = read_tsc();
}

void txn_finish(struct hash_table *hash_table, int s, int status, int mode)
{
  struct txn_ctx *ctx = &hash_table->partitions[s].txn_ctx;
  int nops = ctx->nops;
  int nrels;

  while (--nops >= 0) {
    struct op_ctx *octx = &ctx->op_ctx[nops];
    int t = octx->optype;

    if (!octx->e) {
      // only possible in batch mode
      assert(mode == TXN_BATCH);
      continue;
    }

    if ((octx->e->key & TID_MASK) == ITEM_TID) {
      assert(octx->optype == OPTYPE_LOOKUP);
      continue;
    }

    switch (t) {
      case OPTYPE_LOOKUP:
        // lookups on item table do not need anything
        if ((octx->e->key & TID_MASK) == ITEM_TID)
          continue;

        assert(octx->old_value == NULL);

        // release element
        if (octx->is_local)
          mp_release_value_(&hash_table->partitions[s], octx->e);
        else
          mp_release_value(hash_table, s, octx->e);

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

        plmalloc_free(&hash_table->partitions[s], octx->old_value, size);

        if (octx->is_local)
          mp_release_value_(&hash_table->partitions[s], octx->e);
        else
          mp_mark_ready(hash_table, s, octx->e);

        break;

      case OPTYPE_INSERT:
        // should never get inserts to item table
        assert((octx->e->key & TID_MASK) != ITEM_TID);

        // only single mode supported now
        assert(mode == TXN_SINGLE);
        nrels = (status == TXN_ABORT ? 2 : 1);

        do {
          if (octx->is_local)
            mp_release_value_(&hash_table->partitions[s], octx->e);
          else
            mp_mark_ready(hash_table, s, octx->e);
        } while (--nrels);

        break;
    }
  }

  ctx->nops = 0;
}

void txn_abort(struct hash_table *hash_table, int s, int mode)
{
#if SHARED_NOTHING
  assert(0);
#endif
  return txn_finish(hash_table, s, TXN_ABORT, mode);
}

void txn_commit(struct hash_table *hash_table, int s, int mode)
{
  return txn_finish(hash_table, s, TXN_COMMIT, mode);
}

int run_batch_txn(struct hash_table *hash_table, int s, void *arg)
{
  struct hash_query *query = (struct hash_query *)arg;
  struct partition *p = &hash_table->partitions[s];
  int i, r = TXN_COMMIT;
  int nops = query->nops;
  int nremote = 0;
  int server = -1;
  //void **values = (void **) memalign(CACHELINE, sizeof(void *) * nops);
  //assert(values);
  void *values[MAX_OPS_PER_QUERY];

  txn_start(hash_table, s, TXN_COMMIT);

  /* XXX: REWRITE THIS TO GATHER ALL REMOTE OPS AND SEND IT USING
   * SMP_HASH_DO_ALL 
   */
  // do all local first. if any local fails, no point sending remote requests
  for (i = 0; i < nops; i++) {
    struct hash_op *op = &query->ops[i];    
    server = g_benchmark->hash_get_server(hash_table, op->key);

    // if local, get it
    if (server == s) {
      values[i] = txn_op(hash_table, s, NULL, &query->ops[i], 1);

      if (!values[i]) {
        r = TXN_ABORT;
        goto final;
      }
    }
  }

  // now send all remote requests
  for (i = 0; i < nops; i++) {
    struct hash_op *op = &query->ops[i];    
    server = g_benchmark->hash_get_server(hash_table, op->key);

    if (server != s) {
      if (op->optype == OPTYPE_LOOKUP) {
        smp_hash_lookup(hash_table, s, server, op->key);
        p->nlookups_remote++;
      } else {
        smp_hash_update(hash_table, s, server, op->key);
        p->nupdates_remote++;
      }

      values[i] = NULL;
      nremote++;
    }
  }

  // now get all remote values
  if (nremote) {
    smp_flush_all(hash_table, s);

    struct txn_ctx *ctx = &p->txn_ctx;
    struct box_array *boxes = hash_table->boxes;

    for (i = 0; i < nops; i++) {
      // skip local values we already have
      if (values[i])
        continue;

      uint64_t val;
      struct hash_op *op = &query->ops[i];
      server = g_benchmark->hash_get_server(hash_table, op->key);

      // loop till we get the remote value
      while (buffer_read_all(&boxes[s].boxes[server].out, 1, &val, 0) == 0) {
        process_requests(hash_table, s);
      } 

      struct op_ctx *octx = &ctx->op_ctx[ctx->nops];
      octx->optype = op->optype;
      octx->e = (struct elem *)val;
      octx->is_local = 0;
      octx->old_value = NULL;
      ctx->nops++;

      if (octx->e) {
        int esize = octx->e->size;
        values[i] = octx->e->value;

        if (op->optype == OPTYPE_UPDATE) {
          octx->old_value = plmalloc_alloc(p, esize);
          memcpy(octx->old_value, values[i], esize);
        } else {
          octx->old_value = NULL;
        }
      } else {
        values[i] = NULL;
        r = TXN_ABORT;
#if GATHER_STATS
        p->naborts_remote++;
#endif
      }

      nremote--;
    }

    assert(nremote == 0);
  }

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
    txn_commit(hash_table, s, TXN_BATCH);
  else
    txn_abort(hash_table, s, TXN_BATCH);

  //free(values);

  return r;
}

void process_requests(struct hash_table *hash_table, int s)
{
  struct box_array *boxes = hash_table->boxes;
  uint64_t localbuf[ONEWAY_BUFFER_SIZE];
  struct partition *p = &hash_table->partitions[s];
  int nservers = hash_table->nservers;
  int s_coreid = hash_table->thread_data[s].core;
  char skip_list[BITNSLOTS(MAX_SERVERS)];
  char waiting = 0;
  char continue_loop = 0;

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

  for (int i = 0; i < nservers; i++) {
    if (i == s)
      continue;

    if (BITTEST(skip_list, i)) {
      continue;
    }

    int k = 0;
    int nreleases = 0;
    uint64_t req_ts = hash_table->partitions[i].txn_ctx.ts;
    struct onewaybuffer *b = &boxes[i].boxes[s].in; 
    int count = b->wr_index - b->rd_index;
    if (count == 0) 
      continue;

    //count = buffer_read_all(b, ONEWAY_BUFFER_SIZE, localbuf, 0);
    count = buffer_peek(b, ONEWAY_BUFFER_SIZE, localbuf);
    assert(count);

    /* process all release messages first */
    while (k < count) {
      if ((localbuf[k] & HASHOP_MASK) != HASHOP_RELEASE)
        break;

      struct elem *e = (struct elem *)(localbuf[k] & (~HASHOP_MASK));

      dprint("srv(%d): cl %d before release %" PRIu64 " rc %" PRIu64 "\n", 
          s, i, e->key, e->ref_count);

#if ENABLE_WAIT_DIE_CC
      wait_die_release(s, p, i, e);
#else
      no_wait_release(p, e);
#endif

#if 0
      // find out lock on the owners list
      struct lock_entry *l;
      TAILQ_FOREACH(l, &e->owners, next) {
        if (l->s == i)
          break;
      }

      if (l) {
        // free lock
        dprint("srv(%d): cl %d key %" PRIu64 " rc %" PRIu64 
          " being removed from owners\n", s, i, e->key, e->ref_count);

        TAILQ_REMOVE(&e->owners, l, next);

        mp_release_value_(p, e);
      } else {
        // it is possible that lock is on waiters list. Imagine a scenario
        // where we send requests in bulk to 2 servers, one waits and other
        // aborts. In this case, we will get release message for a request
        // that is currently waiting
        TAILQ_FOREACH(l, &e->waiters, next) {
          if (l->s == i)
            break;
        }

        // can't be on neither owners nor waiters!
        assert(l);
        TAILQ_REMOVE(&e->waiters, l, next);
      }

      plmalloc_free(p, l, sizeof(struct lock_entry));

      dprint("srv(%d): cl %d post release %" PRIu64 " rc %" PRIu64 "\n", 
          s, i, e->key, e->ref_count);

      // if there are no more owners, then refcount should be 1
      if (TAILQ_EMPTY(&e->owners)) {
        if (e->ref_count != 1) {
          printf("found key %"PRIu64" with ref count %d\n", e->key, e->ref_count);
          fflush(stdout);
        }

        assert(e->ref_count == 1);
      }

      /* If lock_free is set, that means the new lock mode is decided by 
       * the head of waiter list. If lock_free is not set, we still have
       * some readers. So only pending readers can be allowed. Keep 
       * popping items from wait list as long as we have readers.
       */
      l = TAILQ_FIRST(&e->waiters);
      while (l) {
        char conflict = 0;

        if (l->optype == OPTYPE_LOOKUP) {
          conflict = !is_value_ready(e);
        } else {
          conflict = (!is_value_ready(e)) || ((e->ref_count & ~DATA_READY_MASK) > 1);
        }

        if (conflict) {
          break;
        } else {
          /* there's no conflict only if there is a shared lock and we're 
           * requesting a shared lock, or if there's no lock
           */
          assert((e->ref_count & DATA_READY_MASK) == 0);

          if (l->optype == OPTYPE_LOOKUP) {
            assert((e->ref_count & ~DATA_READY_MASK) >= 1);
            e->ref_count++;
          } else {
            assert((e->ref_count & ~DATA_READY_MASK) == 1);
            e->ref_count = DATA_READY_MASK | 2;
          }
        }

        // remove from waiters, add to owners, mark as ready
        TAILQ_REMOVE(&e->waiters, l, next);
        TAILQ_INSERT_HEAD(&e->owners, l, next);

        dprint("srv(%d): release lock request for key %"PRIu64" marking %d as ready\n", 
            s, e->key, l->s);

        // go to next element
        l = TAILQ_FIRST(&e->waiters);
      }
#endif
      k++;
      nreleases++;

    }

    buffer_seek(&boxes[i].boxes[s].in, k);
    continue_loop = 1;
    waiting = 0;

    struct elem *e[ONEWAY_BUFFER_SIZE];
    struct lock_entry *locks[ONEWAY_BUFFER_SIZE];
    int j = 0;
    int r = 0;

    while (k < count && continue_loop) {

      locks[j] = NULL;
      uint64_t optype = localbuf[k] & HASHOP_MASK;

      switch (optype) {
#if 0
        case HASHOP_LOOKUP:
          {
            dprint("srv (%d): cl %d lookup %" PRIu64 "\n", s, i, 
              localbuf[k] & (~HASHOP_MASK));

            e[j] = hash_lookup(p, localbuf[k] & (~HASHOP_MASK));
            if (!e[j]) {
              dprint("srv (%d): cl %d lookup FAILED %" PRIu64 "\n", s, i, 
                localbuf[k] & (~HASHOP_MASK));

              assert(0);
            }

            dprint("srv (%d): cl %d lookup %" PRIu64 " rc %" PRIu64 "\n", s, i,
              localbuf[k] & (~HASHOP_MASK), e[j]->ref_count);

            if (!is_value_ready(e[j])) {
              assert(e[j]->ref_count == (DATA_READY_MASK | 2));

              dprint("srv (%d): cl %d lookup %" PRIu64 " rc %" PRIu64 " aborted \n", s, i,
                localbuf[k] & (~HASHOP_MASK), e[j]->ref_count);

              break;

            } 

            e[j]->ref_count++;

            localbuf[j] = (unsigned long)e[j];
            k++;
            j++;

            break;
          }
#endif
        case HASHOP_INSERT:
          {
#if GATHER_STATS
            p->ninserts++;
#endif
            e[j] = hash_insert(p, localbuf[k] & (~HASHOP_MASK), 
              localbuf[k + 1], NULL);

            if (e[j] != NULL) {
              e[j]->ref_count = DATA_READY_MASK | 2; 
            }
            k += INSERT_MSG_LENGTH;
            buffer_seek(b, INSERT_MSG_LENGTH);

            localbuf[j] = (unsigned long) e[j];
            j++;
            break;
          }
        case HASHOP_LOOKUP:
        case HASHOP_UPDATE:
          {
            struct lock_entry *l;
            char conflict = 0;

            dprint("srv (%d): cl %d update %" PRIu64 "\n", s, i, 
              localbuf[k] & (~HASHOP_MASK));

            e[j] = hash_lookup(p, localbuf[k] & (~HASHOP_MASK));
            if (!e[j]) {
              printf("srv (%d): cl %d update %" PRIu64 "\n", s, i, 
                localbuf[k] & (~HASHOP_MASK));
              assert (0);
            }

#if ENABLE_WAIT_DIE_CC
            r = wait_die_acquire(s, p, i, e[j], 
                optype == HASHOP_LOOKUP ? OPTYPE_LOOKUP : OPTYPE_UPDATE, 
                req_ts, &l);
#else
            r = no_wait_acquire(e[j], 
                optype == HASHOP_LOOKUP ? OPTYPE_LOOKUP : OPTYPE_UPDATE);
#endif
            if (r == LOCK_SUCCESS) {
              localbuf[j] = (unsigned long)e[j];
              locks[j] = l;
            } else if (r == LOCK_WAIT) {
#if !defined(ENABLE_WAIT_DIE_CC)
              assert(0);
#endif
              waiting = 1;
              localbuf[j] = 0;
              locks[j] = l;
            } else {
              continue_loop = 0;
              assert(r == LOCK_ABORT);
              break;
            }

#if 0
            /* If we are on the owner's list, we have the element, ref counts are all
             * set, just save it and move along 
             */
            TAILQ_FOREACH(l, &e[j]->owners, next) {
              if (l->s == i)
                break;
            }

            if (l) {
              dprint("srv(%d): cl %d update %" PRIu64 " rc %" PRIu64 
                  " found in owner\n", s, i, localbuf[k] & (~HASHOP_MASK), 
                  e[j]->ref_count);

              localbuf[j] = (unsigned long)e[j];

              locks[j] = l;

              goto have_data; 
            }

            dprint("srv(%d): cl %d update %" PRIu64 " rc %" PRIu64 "\n", s, 
                i, localbuf[k] & (~HASHOP_MASK), e[j]->ref_count);

            conflict = !is_value_ready(e[j]) || e[j]->ref_count > 1;

            /* with wait die, we also have a conflict if there is a waiting list
             * and if head of waiting list is a newer txn than incoming one
             */
            if (!conflict) {
              if (!TAILQ_EMPTY(&e[j]->waiters)) {
                l = TAILQ_FIRST(&e[j]->waiters);
                if (req_ts <= l->ts)
                  conflict = 1;
              }
            }
              
            /* if somebody has lock or wait die is signaling a conflict
             * we need to check if this txn must wait or abort. If it must 
             * wait, then we should add it to waiters list and continue. 
             * SOmetime later, someone will move us to the owner list. At
             * that point, when we retry getting lock again, we will succeed.
             * So we will reply back only at that time. For now, we should
             * leave messages in queue and not remove them.
             *
             * If it must abort, then we should unlock all locks acquired,
             * then return back a NULL for each message.
             
             * XXX: We rely on state maintained in messaging system to ensure
             * that same ops are retried later. Given that we maintain an
             * explicit waiter list, another option would be to use async
             * messaging to send out a lock acquired response when the lock
             * is released. But doing this means that requests can get replied
             * to out of order. This is something messaging system cannot
             * handle now. 
             */
            if (conflict) {
              /* There was a conflict. In wait die case, we can wait if 
               * req_ts is < ts of all owner txns 
               */
              char wait = 1;
              TAILQ_FOREACH(l, &e[j]->owners, next) {
                if (l->ts < req_ts || ((l->ts == req_ts) && (l->s < i))) {
                  wait = 0;
                  break;
                }
              }

              if (wait) {

                waiting = 1;

                dprint("srv(%d): cl %d update %"PRIu64" rc %"PRIu64" waiting \n", 
                  s, i, localbuf[k] & (~HASHOP_MASK), e[j]->ref_count);

                // if we are allowed to wait, make a new lock entry and add it to
                // waiters list. Its possible that the request is being
                // retriedin which case it would already be on the waiters list
                char already_waiting = 0;
                TAILQ_FOREACH(l, &e[j]->waiters, next) {
                  // break if we're already waiting
                  if (l->ts == req_ts && l->s == i) {
                    assert(l->optype == OPTYPE_UPDATE);
                    already_waiting = 1;
                    locks[j] = l;
                    break;
                  }

                  // break if this is where we have to insert ourself 
                  if (l->ts < req_ts || (l->ts == req_ts && l->s < i))
                    break;
                }

                if (!already_waiting) {
                  struct lock_entry *target = plmalloc_alloc(p, sizeof(struct lock_entry));
                  assert(target);
                  target->ts = req_ts;
                  target->s = i;
                  target->optype = OPTYPE_UPDATE;
                  target->ready = 0;
                  locks[j] = target;

                  if (l) {
                    TAILQ_INSERT_BEFORE(l, target, next);
                  } else {
                    if (TAILQ_EMPTY(&e[j]->waiters)) {
                      TAILQ_INSERT_HEAD(&e[j]->waiters, target, next);
                    } else {
                      TAILQ_INSERT_TAIL(&e[j]->waiters, target, next);
                    }
                  }
                }

                localbuf[j] = 0;

              } else {
                dprint("srv(%d): cl %d update %"PRIu64" rc %"PRIu64" aborted \n", 
                  s, i, localbuf[k] & (~HASHOP_MASK), e[j]->ref_count);

                continue_loop = 0;

                // stop processing loop. We have an abort case
                break;
              }
            } else {

              e[j]->ref_count = DATA_READY_MASK | 2;

              // insert a lock in the owners list
              struct lock_entry *target = plmalloc_alloc(p, sizeof(struct lock_entry));
              assert(target);
              target->ts = req_ts;
              target->s = i;
              target->optype = OPTYPE_UPDATE;
              target->ready = 1;
              locks[j] = target;

              dprint("srv(%d): cl %d update %" PRIu64 " rc %" PRIu64 
                  " adding to owners\n", s, i, localbuf[k] & (~HASHOP_MASK), 
                  e[j]->ref_count);

              TAILQ_INSERT_HEAD(&e[j]->owners, target, next);

              localbuf[j] = (unsigned long)e[j];
            }

have_data:
#endif
            assert(r != LOCK_ABORT);
            k++;
            j++;
            break;
          }
        default:
          printf("cl %d invalid message type %lx\n", i, (localbuf[k] & HASHOP_MASK) >> 32); 
          fflush(stdout);
          assert(0);
          break;
      }
    }

    if (continue_loop == 0) {// abort case
      // unlock everything acquired
      if (j > 0) {
        while (--j >= 0) {
          dprint("srv(%d): cl %d update %" PRIu64 " rc %" PRIu64" "
              " non owner releasing\n", s, i, e[j]->key,
              e[j]->ref_count);

#if ENABLE_WAIT_DIE_CC
          assert(locks[j]);

          if (localbuf[j]) { // owning case 
            TAILQ_REMOVE(&e[j]->owners, locks[j], next);
            mp_release_value_(p, e[j]);
          } else { // waiting case
            TAILQ_REMOVE(&e[j]->waiters, locks[j], next);
          }

          plmalloc_free(p, locks[j], sizeof(struct lock_entry));
#else
          mp_release_value_(p, e[j]);
#endif
        }
      }

      // fill up localbuf will NULL
      for (k = 0; k < count - nreleases; k++)
        localbuf[k] = 0;

      j = count - nreleases;
    }

    /* both in case of aborts and success, send back data to the caller and 
    * mark all messages as read.
    * If we are waiting on even one message, then don't do either.
    */
    if ((waiting == 0 || continue_loop == 0) && j > 0) {

      if (!(localbuf[0] || continue_loop == 0)) {
        dprint("YIKES srv(%d): cl %d\n", s, i);
      }

      assert(localbuf[0] || continue_loop == 0);

      buffer_seek(&boxes[i].boxes[s].in, j);
      buffer_write_all(&boxes[i].boxes[s].out, j, localbuf, 1);
    }
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

int smp_hash_lookup(struct hash_table *hash_table, int client_id, 
    int server, hash_key key)
{
  uint64_t msg_data = key | HASHOP_LOOKUP;

  assert(server >= 0 && server < hash_table->nservers);

  dprint("srv(%d): sending lookup for key %"PRIu64" to srv %d\n", client_id, key, server);

  buffer_write(&hash_table->boxes[client_id].boxes[server].in, msg_data);

  return 1;
}

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

int smp_hash_update(struct hash_table *hash_table, int client_id, int server, hash_key key)
{
  uint64_t msg_data = key | HASHOP_UPDATE;

  assert(server >= 0 && server < hash_table->nservers);

  dprint("srv(%d): sending update for key %"PRIu64" to srv %d\n", client_id, key, server);

  buffer_write(&hash_table->boxes[client_id].boxes[server].in, msg_data);

  return 1;
}

void smp_hash_doall(struct hash_table *hash_table, int client_id, int nqueries, 
  struct hash_op **queries, void **values)
{
  int r, i;
  uint64_t val;

  struct box_array *boxes = hash_table->boxes;
  uint64_t msg_data;

  for(int i = 0; i < nqueries; i++) {
    int s = g_benchmark->hash_get_server(hash_table, queries[i]->key);

    dprint("srv(%d): issue remote op %s key %" PRIu64 " to %d\n", 
      client_id, queries[i]->optype == OPTYPE_LOOKUP ? "lookup":"update", 
      queries[i]->key, s);

    switch (queries[i]->optype) {
      case OPTYPE_LOOKUP:
        msg_data = (uint64_t)queries[i]->key | HASHOP_LOOKUP;
        buffer_write_all(&boxes[client_id].boxes[s].in, 1, &msg_data, 0);
#if GATHER_STATS
        hash_table->partitions[client_id].nlookups_remote++;
#endif
        break;
      case OPTYPE_UPDATE:
        msg_data = (uint64_t)queries[i]->key | HASHOP_UPDATE;
        buffer_write_all(&boxes[client_id].boxes[s].in, 1, &msg_data, 0);
#if GATHER_STATS
        hash_table->partitions[client_id].nupdates_remote++;
#endif
        break;
      default:
        assert(0);
        break;
    }
  }


  // after queueing all the queries we flush all buffers and read all remaining values
  smp_flush_all(hash_table, client_id);

  i = 0;
  while (i < nqueries) {
    int ps = g_benchmark->hash_get_server(hash_table, queries[i]->key);
    r = buffer_read_all(&boxes[client_id].boxes[ps].out, 1, &val, 0);

    if (r) {
      values[i] = (void *)(unsigned long)val;
      i++;
    }

    process_requests(hash_table, client_id);
  }
}

void smp_flush_all(struct hash_table *hash_table, int client_id)
{
  for (int i = 0; i < hash_table->nservers; i++) {
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
#if SHARED_EVERYTHING
  selock_release(p, e);
#else
  e->ref_count = (e->ref_count & (~DATA_READY_MASK)) - 1;
#endif

  dprint("srv(%ld): Releasing key %" PRIu64 " rc %" PRIu64 "\n", 
      p - hash_table->partitions, e->key,
      e->ref_count);

  if (e->ref_count == 0) {
    //printf("key %" PRIu64 " 0 rc\n", e->key);
    assert(0);
    hash_remove(p, e);
  }
}

void mp_send_release_msg_(struct hash_table *hash_table, int client_id, void *ptr, int force_flush)
{
  struct elem *e = (struct elem *)ptr;
  uint64_t msg_data = (uint64_t)e | HASHOP_RELEASE;
  assert((uint64_t)e == (msg_data & (~HASHOP_MASK)));

  int s = g_benchmark->hash_get_server(hash_table, e->key);
  buffer_write_all(&hash_table->boxes[client_id].boxes[s].in, 1, &msg_data, 
          force_flush);
}

void mp_release_value(struct hash_table *hash_table, int client_id, void *ptr)
{
  mp_send_release_msg_(hash_table, client_id, ptr, 1 /* force_flush */);
}

void mp_mark_ready(struct hash_table *hash_table, int client_id, void *ptr)
{
#if SHARED_EVERYTHING
  assert(0);
#else

  dprint("srv(%d): sending release msg key %" PRIu64 " rc %" PRIu64 " \n", 
      client_id, ((struct elem *)ptr)->key, ((struct elem*)ptr)->ref_count);

  mp_send_release_msg_(hash_table, client_id, ptr, 1 /* force_flush */);
#endif
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

int stats_get_nlookups(struct hash_table *hash_table)
{
  int nlookups = 0;
  for (int i = 0; i < hash_table->nservers; i++) {
    printf("srv %d lookups local: %d remote %d \n", i, 
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
    e = TAILQ_FIRST(&p->table[i].chain);
    while (e != NULL) {
      nelems++;
      e = TAILQ_NEXT(e, chain);
    }
  }
  *avg = (double)nelems / p->nhash;
  *stddev = 0;

  for (int i = 0; i < p->nhash; i++) {
    e = TAILQ_FIRST(&p->table[i].chain);
    int length = 0;
    while (e != NULL) {
      e = TAILQ_NEXT(e, chain);
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

