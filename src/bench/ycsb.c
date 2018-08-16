#include "headers.h"
#include "partition.h"
#include "smphashtable.h"
#include "benchmark.h"
#include "twopl.h"

#define YCSB_NFIELDS 10
#define YCSB_FIELD_SZ 100
#define YCSB_REC_SZ (YCSB_NFIELDS * YCSB_FIELD_SZ)

struct drand48_data *rand_buffer;
double g_zetan;
double g_zeta2;
double g_eta;
double g_alpha_half_pow;

extern void *micro_alloc_query();

double zeta(uint64_t n, double theta) {
	double sum = 0;
	for (uint64_t i = 1; i <= n; i++)
		sum += pow(1.0 / i, theta);
	return sum;
}

void ycsb_init() {
	printf("Initializing zipf\n");
	rand_buffer = (struct drand48_data *) calloc(g_nservers, sizeof(struct drand48_data));
	for (int i = 0; i < g_nservers; i ++) {
		srand48_r(i + 1, &rand_buffer[i]);
	}

	uint64_t n = g_nrecs - 1;
	g_zetan = zeta(n, g_alpha);
	g_zeta2 = zeta(2, g_alpha);

	g_eta = (1 - pow(2.0 / n, 1 - g_alpha)) / (1 - g_zeta2 / g_zetan);
	g_alpha_half_pow = 1 + pow(0.5, g_alpha);
	printf("n = %lu\n", n);
	printf("theta = %.2f\n", g_alpha);
}

void zipf_val(int s, struct hash_op *op) {
	uint64_t n = g_nrecs - 1;

	double alpha = 1 / (1 - g_alpha);

	double u;
	drand48_r(&rand_buffer[s], &u);
	double uz = u * g_zetan;
	if (uz < 1) {
		op->key = 0;
	} else if (uz < g_alpha_half_pow) {
		op->key = 1;
	} else {
		op->key = (uint64_t)(n * pow(g_eta * u - g_eta + 1, alpha));
	}

	// get the server id for the key
	int tserver = op->key % g_startup_servers;
	// get the key count for the key
	uint64_t key_cnt = op->key / g_startup_servers;

	uint64_t recs_per_server = g_nrecs / g_startup_servers;
	op->key = tserver * recs_per_server + key_cnt;

	assert(op->key < g_nrecs);
}

void ycsb_load_data(struct hash_table *hash_table, int id)
{
  struct elem *e;
  uint64_t *value;
  struct partition *p = &hash_table->partitions[id];

#if SHARED_EVERYTHING
    // quick hack to get SE to load data from all threads
  uint64_t nrecs_per_server = p->nrecs / g_startup_servers;
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

    for (int i = 0; i < YCSB_NFIELDS; i++) {
        value = (uint64_t *)(e->value + i * YCSB_FIELD_SZ);
        value[0] = 0;
    }

    e->ref_count = 1;

    qid++;
  }
}

static void sn_make_operation(struct hash_table *hash_table, int s, 
    struct hash_op *op)
{
  struct partition *p = &hash_table->partitions[s];
  uint64_t nrecs_per_server = p->nrecs; 
  uint64_t nrecs = nrecs_per_server * g_startup_servers;

  op->size = 0;
  zipf_val(s, op);
}

static void se_make_operation(struct hash_table *hash_table, int s, 
    struct hash_op *op)
{
  struct partition *p = &hash_table->partitions[s];
  uint64_t nrecs = p->nrecs; 
  uint64_t nrecs_per_server = g_nrecs / g_startup_servers;

  op->size = 0;
  zipf_val(s, op);
}

void ycsb_get_next_query(struct hash_table *hash_table, int s, void *arg)
{
//	printf("Getting next query for server %d\n", s);
  struct hash_query *query = (struct hash_query *) arg;
  struct partition *p = &hash_table->partitions[s];
  int r = URand(&p->seed, 1, 99);
  char is_duplicate = 0, is_ronly = 0;
  uint64_t nrecs_per_server = g_nrecs / g_startup_servers;
  
  query->nops = g_ops_per_txn;

  /* transaction is either all read or all update */
  /* in this mode, we devote a bunch of threads to reads and another set to
   * writes. Read threads do only reads, write only write. In this mode,
   * g_write_threshold indicates the number of writer threads.
   * g_write_threshold = 0 means all readers
   * g_write_threshold = g_nservers means all writers
   */
  assert(g_write_threshold <= g_nservers);
  if (s >= g_write_threshold)
      is_ronly = 1;

  for (int i = 0; i < g_ops_per_txn; i++) {
    struct hash_op *op = &query->ops[i];

    op->optype = (is_ronly ? OPTYPE_LOOKUP : OPTYPE_UPDATE);

    do {
#if SHARED_EVERYTHING
        se_make_operation(hash_table, s, op);
#else
        sn_make_operation(hash_table, s, op);
#endif
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

int ycsb_run_txn(struct hash_table *hash_table, int s, void *arg, 
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
    int tserver = (int)(op->key / (g_nrecs/g_startup_servers));
#else
    int tserver = ((int)(op->key)) / nrecs_per_partition;
#endif
    assert((tserver >= 0) && (tserver < g_nfibers * g_nservers));

    value = txn_op(ctask, hash_table, s, op, tserver);
#if defined(MIGRATION)
    s = ctask->s;
#endif
    if (!value) {
        r = TXN_ABORT;
        break;
    }

    // in both lookup and update, we just check the value
    uint64_t first_field = *(uint64_t *)value;

    for (int j = 1; j < YCSB_NFIELDS; j++) {
        uint64_t *next_field = (uint64_t *)(value + j * YCSB_FIELD_SZ);
        assert(*next_field == first_field);
    }

    if (op->optype == OPTYPE_UPDATE) {
        for (int j = 0; j < YCSB_NFIELDS; j++) {
            uint64_t *next_field = (uint64_t *)(value + j * YCSB_FIELD_SZ);
            next_field[0]++;
        }
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

struct benchmark ycsb_bench = {
  .init = ycsb_init,
  .load_data = ycsb_load_data,
  .alloc_query = micro_alloc_query,
  .get_next_query = ycsb_get_next_query,
  .run_txn = ycsb_run_txn,
  .verify_txn = NULL,
};
