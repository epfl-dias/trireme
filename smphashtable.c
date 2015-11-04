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
#include "onewaybuffer.h"
#include "smphashtable.h"
#include "util.h"

/** 
 * Hash Table Operations
 */
#define HASHOP_MASK       0xF000000000000000
#define HASHOP_LOOKUP     0x1000000000000000 
#define HASHOP_INSERT     0x2000000000000000 
#define HASHOP_UPDATE     0x3000000000000000 
#define HASHOP_RELEASE    0x4000000000000000 

#define INSERT_MSG_LENGTH 2

#define DATA_READY_MASK   0x8000000000000000

#define MAX_PENDING_PER_SERVER  (ONEWAY_BUFFER_SIZE - (CACHELINE >> 3))

#define TXN_BATCH 1
#define TXN_SINGLE 0

static volatile int nready = 0;

extern int ops_per_txn;
extern int batch_size;
extern int niters;
extern double dist_threshold;
extern double write_threshold;
extern double alpha;

/**
 * Server/Client Message Passing Data Structures
 */
struct box {
  struct onewaybuffer in;
  struct onewaybuffer out;
}  __attribute__ ((aligned (CACHELINE)));

struct box_array {
  struct box *boxes;
} __attribute__ ((aligned (CACHELINE)));

struct thread_args {
  int id;
  int core;
  struct hash_table *hash_table;
};

/*
 * Hash Table data structure
 */
struct hash_table {
  int nservers;
  volatile int nclients;
  size_t nrecs;

  pthread_mutex_t create_client_lock;

  volatile int quitting;
  pthread_t *threads;
  struct thread_args *thread_data;

  struct partition *partitions;
  struct box_array *boxes;
  uint64_t *keys;

  // stats
  int track_cpu_usage;
};

// Forward declarations
void *hash_table_server(void* args);
int is_value_ready(struct elem *e);
void mp_release_value_(struct elem *e);
void atomic_release_value_(struct elem *e);

/**
 * hash_get_server: returns server that should handle given key
 */
static inline int hash_get_server(const struct hash_table *hash_table, hash_key key)
{
  //return key % hash_table->nservers;
  return key / hash_table->partitions[0].nrecs;
}

struct hash_table *create_hash_table(size_t nrecs, int nservers)
{
  struct hash_table *hash_table = (struct hash_table *)malloc(sizeof(struct hash_table));
  hash_table->nservers = nservers;
  hash_table->nrecs = nrecs;
  hash_table->partitions = memalign(CACHELINE, nservers * sizeof(struct partition));

  hash_table->nclients = 0;
  pthread_mutex_init(&hash_table->create_client_lock, NULL);

  hash_table->boxes = memalign(CACHELINE, MAX_CLIENTS * sizeof(struct box_array));
  for (int i = 0; i < hash_table->nservers; i++) {
    init_hash_partition(&hash_table->partitions[i], nrecs / nservers, nservers);
  }

  hash_table->threads = (pthread_t *)malloc(nservers * sizeof(pthread_t));
  hash_table->thread_data = (struct thread_args *)malloc(nservers * sizeof(struct thread_args));

  create_hash_table_client(hash_table);

  if (alpha != 0) {
    printf("Generating zipfian distribution: ");
    fflush(stdout);

    hash_table->keys = zipf_get_keys(alpha, nrecs / nservers, niters);
  } else
    hash_table->keys = NULL;

  return hash_table;
}

void destroy_hash_table(struct hash_table *hash_table)
{
  for (int i = 0; i < hash_table->nservers; i++) {
    destroy_hash_partition(&hash_table->partitions[i], atomic_release_value_);
  }
  free(hash_table->partitions);

  for (int i = 0; i < hash_table->nservers; i++) 
    free(hash_table->boxes[i].boxes);
  
  free(hash_table->boxes);

  free(hash_table->threads);
  free(hash_table->thread_data);
  pthread_mutex_destroy(&hash_table->create_client_lock); 
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

void load_data(int id, struct partition *p)
{
  struct elem *e;
  uint64_t *value;

  size_t qid = id * p->nrecs;
  size_t eqid = qid + p->nrecs;
  
  while (qid < eqid) {
    p->ninserts++;
    e = hash_insert(p, qid, YCSB_REC_SZ, mp_release_value_);
    assert(e);

    value = (uint64_t *)e->value;
    for (int i = 0; i < YCSB_NFIELDS; i++) 
      value[i] = qid;

    e->ref_count = 1;

    qid++;
  }

}

void get_next_query(struct hash_table *hash_table, int s, struct hash_query *query)
{
  struct partition *p = &hash_table->partitions[s];
  char is_local;
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

int is_local_op(struct hash_table *hash_table, int s, hash_key key)
{
  int nrecs = hash_table->partitions[s].nrecs;
  int sqid = nrecs * s;
  int eqid = sqid + nrecs;

  //return hash_get_server(hash_table, key) == s;
  return (key >= sqid && key < eqid);
}

struct elem *local_txn_op(struct partition *p, struct hash_op *op)
{
  struct elem *e;

  e = hash_lookup(p, op->key);
  assert(e);

  // if value is not ready, lookup and updates fail
  if (!is_value_ready(e)) {
    assert(e->ref_count == (DATA_READY_MASK | 2));

    dprint("lookup %" PRIu64 " rc %" PRIu64 " aborted \n", op->key,
      e->ref_count);

    p->naborts_local++;

    return NULL;
  }

  // if pending lookups are there, updates fail
  if (op->optype == OPTYPE_UPDATE && e->ref_count > 1) {
    dprint("update %" PRIu64 " rc %" PRIu64 " aborted \n", op->key,
      e->ref_count);

    p->naborts_local++;

    return NULL;
  }

  if (op->optype == OPTYPE_LOOKUP) {
    e->ref_count++;
    p->nlookups_local++;
    p->nhits++;
  } else {
    assert(op->optype == OPTYPE_UPDATE);
    e->ref_count = DATA_READY_MASK | 2; 
    p->nupdates_local++;
  }

  return e;
}

void *txn_op(struct hash_table *hash_table, int s, struct hash_op *op)
{
  void *value = NULL;
  struct elem *e = NULL;
  struct partition *p = &hash_table->partitions[s];
  struct txn_ctx *ctx = &p->txn_ctx;
  int is_local = is_local_op(hash_table, s, op->key);

  dprint("srv(%d): issue %s %s op key %" PRIu64 "\n", 
    s, is_local ? "local":"remote", 
    op->optype == OPTYPE_LOOKUP ? "lookup":"update", op->key);

  // if this is us, just call local procedure
  if (is_local) {
    assert(op->key >= s * p->nrecs && op->key < (s * p->nrecs + p->nrecs));
    
    e = local_txn_op(p, op);

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
  }

  if (value) {
    struct op_ctx *octx = &ctx->op_ctx[ctx->nops];
    octx->optype = op->optype;
    octx->e = e;
    octx->is_local = is_local;

    if (op->optype == OPTYPE_UPDATE) {
      octx->old_value = malloc(e->size);
      assert(octx->old_value);

      memcpy(octx->old_value, e->value, e->size);
    } else {
      octx->old_value = NULL;
    }

    ctx->nops++;
  }

  return value;
}

void txn_start(struct hash_table *hash_table, int s, struct hash_query *query)
{
  struct txn_ctx *ctx = &hash_table->partitions[s].txn_ctx;

  ctx->nops = 0;

  assert(query->nops < MAX_OPS_PER_QUERY);
}

void txn_finish(struct hash_table *hash_table, int s, int status, int mode)
{
  struct txn_ctx *ctx = &hash_table->partitions[s].txn_ctx;
  int nops = ctx->nops;

  while (--nops >= 0) {
    struct op_ctx *octx = &ctx->op_ctx[nops];

    if (octx->optype == OPTYPE_LOOKUP || mode == TXN_BATCH) {
      assert(octx->old_value == NULL || mode == TXN_BATCH);
      if (octx->is_local)
        mp_release_value_(octx->e);      
      else
        mp_release_value(hash_table, s, octx->e);
    } else {
      assert(octx->old_value);
      if (status == TXN_ABORT)
        memcpy(octx->e->value, octx->old_value, octx->e->size);
      free(octx->old_value);
      if (octx->is_local)
        mp_release_value_(octx->e);      
      else
        mp_mark_ready(hash_table, s, octx->e);
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

static int txn_batch_op(struct hash_table *hash_table, int s, 
  struct hash_query *query, void **values)
{
  // We are going to gather all pointers first. Then, we do the
  // updates if necessary. So, there need be no undo logging
  int i, r, nremote;

  struct txn_ctx *tctx = &hash_table->partitions[s].txn_ctx;

  void **eremote = malloc(sizeof(void *) * query->nops);
  assert(eremote);

  struct hash_op **ops = malloc(sizeof(struct op *) * query->nops); 
  assert(ops);

  struct op_ctx **r_o_ctx = malloc(sizeof(struct op_ctx *) * query->nops);
  assert(r_o_ctx);

  r = 1;
  nremote = 0;

  for (i = 0; i < query->nops; i++) {

    struct op_ctx *octx = &tctx->op_ctx[i];
    struct hash_op *op = &query->ops[i];

    octx->optype = op->optype;
    octx->is_local = is_local_op(hash_table, s, op->key);
    octx->old_value = NULL;

    if (octx->is_local) {

      // get local values
      octx->e = local_txn_op(&hash_table->partitions[s], op);
      if (!octx->e) {
        r = 0;
        goto final;
      }

      values[i] = octx->e->value;

      tctx->nops++;

     } else {
      // only gather remote ops for now
      ops[nremote] = op;
      eremote[nremote] = NULL;
      r_o_ctx[nremote] = octx;
      nremote++;
    }
  }

  // now get all remote values 
  smp_hash_doall(hash_table, s, nremote, ops, eremote);

  // prep context
  for (i = 0; i < nremote; i++) {
    r_o_ctx[i]->e = (struct elem *)eremote[i]; 
    assert(eremote[i]);
  }

  // now set all values
  for (i = 0; i < query->nops; i++)
    values[i] = tctx->op_ctx[i].e->value;
    
final:
  free(eremote);
  free(ops);
  free(r_o_ctx);

  return r;
}

static int run_batch_txn(struct hash_table *hash_table, int s, 
  struct hash_query *query)
{
  int i, r;

  void **values = malloc(sizeof(void *) * query->nops);
  assert(values);

  txn_start(hash_table, s, query);

  r = TXN_COMMIT;
  if (!txn_batch_op(hash_table, s, query, values))
    r = TXN_ABORT;

  if (r == TXN_COMMIT) {
    for (i = 0; i < query->nops; i++) {
      // in both lookup and update, we just check the value
      uint64_t *int_val = (uint64_t *)values[i];

      for (int j = 0; j < YCSB_NFIELDS; j++) {
        assert (int_val[j] == query->ops[i].key);
      }
    }

    txn_commit(hash_table, s, TXN_BATCH);
  } else
    txn_abort(hash_table, s, TXN_BATCH);

  free(values);

  return r;
}

static int run_txn(struct hash_table *hash_table, int s, 
  struct hash_query *query)
{
  int i, r;
  void *value;

  txn_start(hash_table, s, query);

  r = TXN_COMMIT;
  for (i = 0; i < query->nops; i++) {
    value = txn_op(hash_table, s, &query->ops[i]);
  
    if (!value) {
      r = TXN_ABORT;
      break;
    }

    // in both lookup and update, we just check the value
    uint64_t *int_val = (uint64_t *)value;

    for (int j = 0; j < YCSB_NFIELDS; j++) {
      assert (int_val[j] == query->ops[i].key);
    }
  }

  if (r == TXN_COMMIT) {
    txn_commit(hash_table, s, TXN_SINGLE);
  } else {
    txn_abort(hash_table, s, TXN_SINGLE);
  }

  return r;
}

void process_requests(struct hash_table *hash_table, int s)
{
  struct box_array *boxes = hash_table->boxes;
  uint64_t localbuf[ONEWAY_BUFFER_SIZE];
  struct partition *p = &hash_table->partitions[s];

  int nclients = hash_table->nservers;

  for (int i = 0; i < nclients; i++) {
      
    int count = buffer_peek(&boxes[i].boxes[s].in, ONEWAY_BUFFER_SIZE, 
      localbuf);

    if (count == 0) continue;

    int k = 0;
    int j = 0;
    int abort = 0;
    int nreleases = 0;
    unsigned long value;

    /* process all release messages first */
    while (k < count) {
        if ((localbuf[k] & HASHOP_MASK) != HASHOP_RELEASE)
          break;

        struct elem *e = (struct elem *)(localbuf[k] & (~HASHOP_MASK));

        dprint("cl %d before release %" PRIu64 " rc %" PRIu64 "\n", i, 
          e->key, e->ref_count);

        mp_release_value_(e);
        k++;
        nreleases++;

        dprint("cl %d post release %" PRIu64 " rc %" PRIu64 "\n", i, 
          e->key, e->ref_count);
    }

    buffer_seek(&boxes[i].boxes[s].in, k);

    struct elem *e[ONEWAY_BUFFER_SIZE];

    while (k < count && !abort) {
      value = 0;

      switch (localbuf[k] & HASHOP_MASK) {
        case HASHOP_LOOKUP:
          {
            dprint("srv (%d): cl %d lookup %" PRIu64 "\n", s, i, 
              localbuf[k] & (~HASHOP_MASK));

            e[j] = hash_lookup(p, localbuf[k] & (~HASHOP_MASK));
            assert(e[j]);

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
              localbuf[k + 1], mp_release_value_);

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
            assert (e[j]);

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
        mp_release_value_(e[j]);
    }
  }
}

void * hash_table_server(void* args)
{
  int i, r;
  const int s = ((struct thread_args *) args)->id;
  const int c = ((struct thread_args *) args)->core;
  struct hash_table *hash_table = ((struct thread_args *) args)->hash_table;
  struct partition *p = &hash_table->partitions[s];
  struct hash_query query;
  int quitting = 0;
  __attribute__((unused)) int pct = 10;

  set_affinity(c);
   
  double tstart = now();

  load_data(s, p);

  double tend = now();

  printf("srv %d load time %.3f\n", s, tend - tstart);
  printf("srv %d rec count: %" PRId64 " tx count: %d, per_txn_op cnt: %d\n", 
    s, p->nrecs, niters, ops_per_txn);

  double avg;
  double stddev;
  stats_get_buckets(hash_table, s, &avg, &stddev);
  printf("srv %d hash table occupancy avg %0.3f stddev %0.3f\n", s, avg, stddev);
 
  fflush(stdout);

  nready++;

  while (nready != hash_table->nservers) ;

  printf("srv %d starting txns\n", s);
  fflush(stdout);

  memset(&query, 0, sizeof(struct hash_query));

  tstart = now();

  for (i = 0; i < niters; i++) {
    // run query as txn
    int naborts = 0;

    get_next_query(hash_table, s, &query);

    do {
      if (batch_size == 1)
        r = run_txn(hash_table, s, &query);
      else
        r = run_batch_txn(hash_table, s, &query);

      // see if we need to answer someone
      process_requests(hash_table, s);

      naborts++;

#ifdef DPROGRESS
      if (naborts % 1000 == 0) {
        printf("srv %d txn %d aborted %d times\n", s, i, naborts);
        fflush(stdout);
      }
#endif

    } while (r == TXN_ABORT);

#ifdef DPROGRESS
    if (niters * pct / 100 == i) {
      printf("srv %d %d%% txn complete\n", s, pct);
      fflush(stdout);
      pct += 10;
    }
#endif

  }

  tend = now();

  printf("srv %d query time %.3f\n", s, tend - tstart);
  fflush(stdout);

  p->tps = niters / (tend - tstart);

  nready--;

  while (nready != 0)
      process_requests(hash_table, s);

  printf("srv %d quitting \n", s);
  fflush(stdout);

  quitting = 1;

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
  int s = hash_get_server(hash_table, key);

  buffer_write(&hash_table->boxes[client_id].boxes[s].in, msg_data);

  return 1;
}

int smp_hash_insert(struct hash_table *hash_table, int client_id, hash_key key, int size)
{
  uint64_t msg_data[INSERT_MSG_LENGTH];
  int s = hash_get_server(hash_table, key);

  msg_data[0] = (unsigned long)key | HASHOP_INSERT;
  msg_data[1] = (unsigned long)size;

  buffer_write_all(&hash_table->boxes[client_id].boxes[s].in, INSERT_MSG_LENGTH, msg_data, 0);

  return 1;
}

int smp_hash_update(struct hash_table *hash_table, int client_id, hash_key key)
{
  uint64_t msg_data = key | HASHOP_UPDATE;
  int s = hash_get_server(hash_table, key);

  buffer_write(&hash_table->boxes[client_id].boxes[s].in, msg_data);

  return 1;
}

void smp_hash_doall(struct hash_table *hash_table, int client_id, int nqueries, 
  struct hash_op **queries, void **values)
{
  int r, pindex = 0;
  uint64_t val;

  int *pending_count = (int*)memalign(CACHELINE, hash_table->nservers * sizeof(int));
  memset(pending_count, 0, hash_table->nservers * sizeof(int));

  struct box_array *boxes = hash_table->boxes;
  uint64_t msg_data[INSERT_MSG_LENGTH];

  for(int i = 0; i < nqueries; i++) {
    int s = hash_get_server(hash_table, queries[i]->key);

    while (pending_count[s] >= MAX_PENDING_PER_SERVER) {
      // we need to read values from server "s" buffer otherwise if we try to write
      // more queries to server "s" it will be blocked trying to write the return value 
      // to the buffer and it can easily cause deadlock between clients and servers
      //
      // however instead of reading server "s"-s buffer immediatelly we will read elements 
      // in same order that they were queued till we reach elements that were queued for 
      // server "s"
      int ps = hash_get_server(hash_table, queries[pindex]->key);
      r = buffer_read_all(&boxes[client_id].boxes[ps].out, 1, &val, 0);
      if (r == 0) {
        buffer_flush(&boxes[client_id].boxes[ps].in);
        r = buffer_read_all(&boxes[client_id].boxes[ps].out, 1, &val, 1);
      }
      values[pindex] = (void *)(unsigned long)val;
      pending_count[ps]--;
      pindex++;
    }

    switch (queries[i]->optype) {
      case OPTYPE_LOOKUP:
        msg_data[0] = (uint64_t)queries[i]->key | HASHOP_LOOKUP;
        buffer_write_all(&boxes[client_id].boxes[s].in, 1, msg_data, 0);
        hash_table->partitions[client_id].nlookups_remote++;
        break;
      case OPTYPE_UPDATE:
        msg_data[0] = (uint64_t)queries[i]->key | HASHOP_UPDATE;
        buffer_write_all(&boxes[client_id].boxes[s].in, 1, msg_data, 0);
        hash_table->partitions[client_id].nupdates_remote++;
        break;
      case OPTYPE_INSERT:
        msg_data[0] = (uint64_t)queries[i]->key | HASHOP_INSERT;
        msg_data[1] = (uint64_t)queries[i]->size;
        buffer_write_all(&boxes[client_id].boxes[s].in, 
          INSERT_MSG_LENGTH, msg_data, 0);
        break;

      default:
        assert(0);
        break;
    }
    pending_count[s]++;
  }

  // after queueing all the queries we flush all buffers and read all remaining values
  for (int i = 0; i < hash_table->nservers; i++) {
    buffer_flush(&boxes[client_id].boxes[i].in);
  }

  while (pindex < nqueries) {
    int ps = hash_get_server(hash_table, queries[pindex]->key);
    r = buffer_read_all(&boxes[client_id].boxes[ps].out, 1, &val, 0);

    if (r) {
      values[pindex] = (void *)(unsigned long)val;
      pending_count[ps]--;
      pindex++;
    }

    process_requests(hash_table, client_id);

  }

  free(pending_count);

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

void mp_release_value_(struct elem *e)
{
  e->ref_count = (e->ref_count & (~DATA_READY_MASK)) - 1;
  if (e->ref_count == 0)
    printf("key %" PRIu64 " 0 rc\n", e->key);

  assert(e->ref_count);
}

void mp_send_release_msg_(struct hash_table *hash_table, int client_id, void *ptr, int force_flush)
{
  struct elem *e = (struct elem *)ptr;
  uint64_t msg_data = (uint64_t)e | HASHOP_RELEASE;
  assert((uint64_t)e == (msg_data & (~HASHOP_MASK)));

  int s = hash_get_server(hash_table, e->key);
  buffer_write_all(&hash_table->boxes[client_id].boxes[s].in, 1, &msg_data, 
          force_flush);
}

void mp_release_value(struct hash_table *hash_table, int client_id, void *ptr)
{
  mp_send_release_msg_(hash_table, client_id, ptr, 0 /* force_flush */);
}

void mp_mark_ready(struct hash_table *hash_table, int client_id, void *ptr)
{
  mp_send_release_msg_(hash_table, client_id, ptr, 1 /* force_flush */);
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


