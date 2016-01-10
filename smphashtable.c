#include <assert.h>
#include <malloc.h>
#include <math.h>
#include <pthread.h>
#include <stdlib.h>
#include <inttypes.h>
#include <string.h>
#include <sys/queue.h>
#include <sys/sysinfo.h>

#include "hashprotocol.h"
#include "partition.h"
#include "smphashtable.h"
#include "util.h"
#include "benchmark.h"
#include "tpcc.h"
#include "selock.h"

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
extern int ops_per_txn;
extern struct hash_table *hash_table;

// Forward declarations
void *hash_table_server(void* args);
int is_value_ready(struct elem *e);
void mp_release_value_(struct partition *p, struct elem *e);
void atomic_release_value_(struct elem *e);
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
  for (i = 0; i < hash_table->nservers; i++) {
    act_psize += hash_table->partitions[i].size;
  }

#endif

  for (i = 0; i < hash_table->nservers; i++) {
    dbg_psize = destroy_hash_partition(&hash_table->partitions[i], 
        atomic_release_value_);

#if SHARED_EVERYTHING
    /* we need to destory buckets for first partition only in case of se */
    assert(act_psize == dbg_psize);
    break;
#endif

  }
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
  int ncpus, coreid = 0;

  ncpus = get_nprocs_conf() / 2;
  assert(ncpus >= hash_table->nservers);

  for (int i = 0; i < hash_table->nservers; i++) {
    hash_table->thread_data[i].id = i;
    hash_table->thread_data[i].core = coreid;
    hash_table->thread_data[i].hash_table = hash_table;

    printf("Assinging core %d to srv %d\n", coreid, i);

    r = pthread_create(&hash_table->threads[i], NULL, hash_table_server, (void *) (&hash_table->thread_data[i]));
    assert(r == 0);

    coreid += 2;
    if (coreid == ncpus) {
      coreid = 1;
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

struct elem *local_txn_op(struct partition *p, struct hash_op *op)
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
      if (!selock_acquire(p, OPTYPE_LOOKUP, e)) {
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

      p->nlookups_local++;
      p->nhits++;
      break;

    case OPTYPE_UPDATE:
      // should not get a update to item partition
      assert (p != hash_table->g_partition);

      e = hash_lookup(p, op->key);
      assert(e);

#if SHARED_EVERYTHING
      if (!selock_acquire(p, OPTYPE_UPDATE, e)) {
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

      p->nupdates_local++;
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
    
    e = local_txn_op(l_p, op);

    // it is possible for a local txn to fail as someone else might have
    // acquired a lock before us
    if (e)
      value = e->value;

  } else {

    // call the corresponding authority and block
    smp_hash_doall(hash_table, s, 1, &op, &value);

    // the remote server blocks us until data is ready. We should never 
    // have the situation where we get a NO notification from remote server
    assert(value);

    e = (struct elem *)value;

    value = e->value;
    assert(value);
  }

  if (value) {
    struct op_ctx *octx = &ctx->op_ctx[ctx->nops];
    octx->optype = op->optype;
    octx->e = e;
    octx->is_local = is_local;

    if (op->optype == OPTYPE_UPDATE) {
      octx->old_value = memalign(CACHELINE, e->size);
      assert(octx->old_value);

      memcpy(octx->old_value, e->value, e->size);
    } else {
      octx->old_value = NULL;
    }

    ctx->nops++;
  }

  return value;
}

void txn_start(struct hash_table *hash_table, int s)
{
  struct txn_ctx *ctx = &hash_table->partitions[s].txn_ctx;

  ctx->nops = 0;
}

void txn_finish(struct hash_table *hash_table, int s, int status, int mode)
{
  struct txn_ctx *ctx = &hash_table->partitions[s].txn_ctx;
  int nops = ctx->nops;
  int nrels;

  while (--nops >= 0) {
    struct op_ctx *octx = &ctx->op_ctx[nops];
    int t = octx->optype;

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

        if (mode == TXN_SINGLE) {
          // if this is a single txn that must be aborted, rollback
          if (status == TXN_ABORT) {
            memcpy(octx->e->value, octx->old_value, octx->e->size);
          }

          assert(octx->old_value);
          free(octx->old_value);
        }

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
  return txn_finish(hash_table, s, TXN_ABORT, mode);
}

void txn_commit(struct hash_table *hash_table, int s, int mode)
{
  return txn_finish(hash_table, s, TXN_COMMIT, mode);
}

__attribute__ ((unused)) static int run_batch_txn(struct hash_table *hash_table, int s, 
  struct hash_query *query)
{
  int i, r, nops, nremote, server;

  void **values = (void **) memalign(CACHELINE, sizeof(void *) * query->nops);
  assert(values);

  txn_start(hash_table, s);

  r = TXN_COMMIT;
  nops = query->nops;

  nremote = 0;
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
    } else {
      // remote op. Just issue it. We'll collect later
      if (op->optype == OPTYPE_LOOKUP)
        smp_hash_lookup(hash_table, s, op->key);
      else
        smp_hash_update(hash_table, s, op->key);

      values[i] = NULL;
      nremote++;
    }
  }

  hash_table->partitions[s].nlookups_remote += nremote;

  if (nremote) {
    // now get all remote values
    smp_flush_all(hash_table, s);

    struct txn_ctx *ctx = &hash_table->partitions[s].txn_ctx;
    struct box_array *boxes = hash_table->boxes;

    for (i = 0; i < nops; i++) {
      // skip local values we already have
      if (values[i])
        continue;

      uint64_t val;
      struct hash_op *op = &query->ops[i];

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

      values[i] = octx->e->value;

      nremote--;
    }

    assert(nremote == 0);
  }

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

  free(values);

  return r;
}

void process_requests(struct hash_table *hash_table, int s)
{
  struct box_array *boxes = hash_table->boxes;
  uint64_t localbuf[ONEWAY_BUFFER_SIZE];
  struct partition *p = &hash_table->partitions[s];

  int nclients = hash_table->nservers;

  for (int i = 0; i < nclients; i++) {
      
    if (i == s)
      continue;

    int count = buffer_peek(&boxes[i].boxes[s].in, ONEWAY_BUFFER_SIZE, 
      localbuf);

    if (count == 0) continue;

    int k = 0;
    int j = 0;
    int abort = 0;
    int nreleases = 0;

    /* process all release messages first */
    while (k < count) {
        if ((localbuf[k] & HASHOP_MASK) != HASHOP_RELEASE)
          break;

        struct elem *e = (struct elem *)(localbuf[k] & (~HASHOP_MASK));

        dprint("srv(%d): cl %d before release %" PRIu64 " rc %" PRIu64 "\n", 
            s, i, e->key, e->ref_count);

        mp_release_value_(p, e);
        k++;
        nreleases++;

        dprint("srv(%d): cl %d post release %" PRIu64 " rc %" PRIu64 "\n", 
            s, i, e->key, e->ref_count);
    }

    buffer_seek(&boxes[i].boxes[s].in, k);

    struct elem *e[ONEWAY_BUFFER_SIZE];

    while (k < count && !abort) {

      switch (localbuf[k] & HASHOP_MASK) {
        case HASHOP_LOOKUP:
          {
            dprint("srv (%d): cl %d lookup %" PRIu64 "\n", s, i, 
              localbuf[k] & (~HASHOP_MASK));

            e[j] = hash_lookup(p, localbuf[k] & (~HASHOP_MASK));
            if (!e[j]) {
              dprint("srv (%d): cl %d lookup FAILED %" PRIu64 "\n", s, i, 
                localbuf[k] & (~HASHOP_MASK));

              assert(e[j]);
            }

            dprint("srv (%d) cl %d lookup %" PRIu64 " rc %" PRIu64 "\n", s, i,
              localbuf[k] & (~HASHOP_MASK), e[j]->ref_count);

            if (!is_value_ready(e[j])) {
              assert(e[j]->ref_count == (DATA_READY_MASK | 2));
              abort = 1;

              dprint("srv (%d) cl %d lookup %" PRIu64 " rc %" PRIu64 " aborted \n", s, i,
                localbuf[k] & (~HASHOP_MASK), e[j]->ref_count);

              break;
            }

            p->nhits++;
            e[j]->ref_count++;
            localbuf[j] = (unsigned long)e[j];
            k++;
            j++;
            break;
          }
        case HASHOP_INSERT:
          {
            p->ninserts++;
            e[j] = hash_insert(p, localbuf[k] & (~HASHOP_MASK), 
              localbuf[k + 1], NULL);

            if (e[j] != NULL) {
              e[j]->ref_count = DATA_READY_MASK | 2; 
            }
            k += INSERT_MSG_LENGTH;

            localbuf[j] = (unsigned long) e[j];
            j++;
            break;
          }
        case HASHOP_UPDATE:
          {
            dprint("srv (%d): cl %d update %" PRIu64 "\n", s, i, 
              localbuf[k] & (~HASHOP_MASK));

            e[j] = hash_lookup(p, localbuf[k] & (~HASHOP_MASK));
            if (!e[j]) {
              printf("srv (%d): cl %d update %" PRIu64 "\n", s, i, 
                localbuf[k] & (~HASHOP_MASK));
              assert (e[j]);
            }

            dprint("cl %d update %" PRIu64 " rc %" PRIu64 "\n", i, 
              localbuf[k] & (~HASHOP_MASK), e[j]->ref_count);

            // if somebody has lock, we can't do anything
            if (!is_value_ready(e[j]) || e[j]->ref_count > 1) {
              abort = 1;

              dprint("cl %d update %" PRIu64 " rc %" PRIu64 " aborted \n", i,
                localbuf[k] & (~HASHOP_MASK), e[j]->ref_count);

              break;
            }

            e[j]->ref_count = DATA_READY_MASK | 2;
            localbuf[j] = (unsigned long)e[j];

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

    // if everything was successful, we move forward
    if (!abort) {
      buffer_seek(&boxes[i].boxes[s].in, count - nreleases);

      buffer_write_all(&boxes[i].boxes[s].out, j, localbuf, 1);
    } else if (j > 0) {
      // abort: rollback all the reference counts
      while (--j >= 0)
        mp_release_value_(p, e[j]);
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

  double avg;
  double stddev;
  stats_get_buckets(hash_table, s, &avg, &stddev);
  printf("srv %d hash table occupancy avg %0.3f stddev %0.3f\n", s, avg, stddev);
 
  fflush(stdout);

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

    do {
      r = g_benchmark->run_txn(hash_table, s, query);
#if 0
      if (batch_size == 1)
        r = run_txn(hash_table, s, &query);
      else
        r = run_batch_txn(hash_table, s, &query);
#endif

      // see if we need to answer someone
#ifndef SHARED_EVERYTHING
      process_requests(hash_table, s);
#endif

      if (r == TXN_ABORT)
        dprint("srv(%d): rerunning aborted txn %d\n", s, i);

    } while (r == TXN_ABORT);

#if PRINT_PROGRESS
    if (i % 100000 == 0)
      printf("srv(%d): completed txn %d\n", s, i);
#endif

  }

  tend = now();

  printf("srv %d query time %.3f\n", s, tend - tstart);
  fflush(stdout);

  p->tps = niters / (tend - tstart);

  nready--;

  while (nready != 0)
#if SHARED_EVERYTHING
    ;
#else
    process_requests(hash_table, s);
#endif

  printf("srv %d quitting \n", s);
  fflush(stdout);

  if (g_benchmark->verify_txn)
    g_benchmark->verify_txn(hash_table, s);

 #if 0
  // stay running until someone else needs us
  while (quitting == 0) {
    quitting = hash_table->quitting;
    process_requests(hash_table, s);
  }
#endif

  return NULL;
}

int smp_hash_lookup(struct hash_table *hash_table, int client_id, hash_key key)
{
  uint64_t msg_data = key | HASHOP_LOOKUP;
  int s = g_benchmark->hash_get_server(hash_table, key);

  buffer_write(&hash_table->boxes[client_id].boxes[s].in, msg_data);

  return 1;
}

int smp_hash_insert(struct hash_table *hash_table, int client_id, hash_key key, int size)
{
  uint64_t msg_data[INSERT_MSG_LENGTH];
  int s = g_benchmark->hash_get_server(hash_table, key);

  msg_data[0] = (unsigned long)key | HASHOP_INSERT;
  msg_data[1] = (unsigned long)size;

  buffer_write_all(&hash_table->boxes[client_id].boxes[s].in, INSERT_MSG_LENGTH, msg_data, 0);

  return 1;
}

int smp_hash_update(struct hash_table *hash_table, int client_id, hash_key key)
{
  uint64_t msg_data = key | HASHOP_UPDATE;
  int s = g_benchmark->hash_get_server(hash_table, key);

  buffer_write(&hash_table->boxes[client_id].boxes[s].in, msg_data);

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
        hash_table->partitions[client_id].nlookups_remote++;
        break;
      case OPTYPE_UPDATE:
        msg_data = (uint64_t)queries[i]->key | HASHOP_UPDATE;
        buffer_write_all(&boxes[client_id].boxes[s].in, 1, &msg_data, 0);
        hash_table->partitions[client_id].nupdates_remote++;
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

  dprint("srv(%ld): Releasing key %" PRIu64 " rc %" PRIu64 "\n", 
      p - hash_table->partitions, e->key,
      e->ref_count);

  if (e->ref_count == 0) {
    //printf("key %" PRIu64 " 0 rc\n", e->key);
    hash_remove(p, e);
  }
#endif
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
  mp_send_release_msg_(hash_table, client_id, ptr, 1 /* force_flush */);
#endif
}

void atomic_release_value_(struct elem *e)
{
  uint64_t ref_count = __sync_sub_and_fetch(&(e->ref_count), 1);
  if (ref_count == 0) {
    if (e->value != (char *)e->local_values)
      free(e->value);

    free(e);
  }
}

void atomic_release_value(void *ptr)
{
  struct elem *e = (struct elem *)(ptr - sizeof(struct elem));
  atomic_release_value_(e);
}

void atomic_mark_ready(void *ptr)
{
  struct elem *e = (struct elem *)(ptr - sizeof(struct elem));
  uint64_t ref_count = __sync_and_and_fetch(&(e->ref_count), (~DATA_READY_MASK));
  if (ref_count == 0) {
    free(e);
  }
}

/**
 * Hash Table Counters and Stats
 */
void stats_reset(struct hash_table *hash_table)
{
  for (int i = 0; i < hash_table->nservers; i++) {
    hash_table->partitions[i].nhits = 0;
    hash_table->partitions[i].ninserts = 0;
    hash_table->partitions[i].nlookups_local = 0;
    hash_table->partitions[i].nupdates_local = 0;
    hash_table->partitions[i].naborts_local = 0;
    hash_table->partitions[i].nlookups_remote = 0;
    hash_table->partitions[i].nupdates_remote = 0;
    hash_table->partitions[i].naborts_remote = 0;
  }
}

int stats_get_nhits(struct hash_table *hash_table)
{
  int nhits = 0;
  for (int i = 0; i < hash_table->nservers; i++) {
    nhits += hash_table->partitions[i].nhits;
  }
  return nhits;
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


