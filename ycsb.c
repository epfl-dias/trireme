#include "ia32perf.h"
#include "headers.h"
#include "benchmark.h"
#include "hashprotocol.h"
#include "smphashtable.h"

#if defined(INTEL64)
  // Event Select values for Intel I7 core processor
  // Counter Mask (8 bits) - INV - EN - ANY - INT - PC - E - OS - USR - UMASK (8 bits) - Event Select (8 bits)
  #define NEVT 2
  uint64_t evts[NEVT] = {
    0x00410224, // L2 Misses
    0x0041412E, // L2 Misses
  };
#elif defined(AMD64)
  // Reserved (22 bits) - HO - GO - Reserved (4 bits) - Event Select (8 bits)
  // Counter Mask (8 bits) - INV - EN - ANY - INT - PC - E - OS - USR - UMASK (8 bits) - Event Select (8 bits)
  #define NEVT 2
  uint64_t evts[NEVT] = {
    0x000041077E, // L2 Misses
    0x04004107E1, // L3 Misses, needs to be ORed with (core# << (12))
  };
#else
  #define NEVT 0
#endif

int queries_per_txn = 1;
int nservers        = 1;
int nclients        = 1;
int first_core      = -1;
int batch_size      = 1;
int ops_per_txn     = 1;
int niters          = 1000000;
size_t size         = 1000000;
int query_mask      = (1 << 29) - 1;
int query_shift     = 2;
double write_threshold = ((double)0.2 * RAND_MAX);
double dist_threshold = ((double) 0.1 * RAND_MAX);
double alpha = 0;
double hot_fraction = 0;
int nhot_servers = 0;

int track_cpu_usage = 0;
int QID[MAX_CLIENTS];

struct hash_table *hash_table;
struct benchmark *g_benchmark;
int iters_per_client; 

uint64_t pmccount[NEVT][MAX_SERVERS + MAX_CLIENTS];
uint64_t pmclast[NEVT][MAX_SERVERS + MAX_CLIENTS];

struct client_data {
  unsigned int seed;
} __attribute__ ((aligned (CACHELINE)));
struct client_data *cdata;

void run_benchmark();
void get_random_query(int client_id, struct hash_query *query);
void get_next_query(int client_id, struct hash_query *query);
void get_mixed_query(int client_id, struct hash_query *query);
void (*qgen)(int client_id, struct hash_query *query);

int main(int argc, char *argv[])
{
  int opt_char;

  while((opt_char = getopt(argc, argv, "a:s:c:f:i:n:t:m:w:d:f:b:e:u:o:h:p:")) != -1) {
    switch (opt_char) {
      case 'a':
        alpha = atof(optarg);
        break;
      case 'h':
        hot_fraction = atof(optarg);
        break;
      case 'p':
        nhot_servers = atoi(optarg);
        break;
      case 's':
        nservers = atoi(optarg);
        assert(nservers < MAX_SERVERS);
        break;
      case 'c':
        nclients = atoi(optarg);
        assert(nclients < MAX_CLIENTS);
        break;
      case 'f':
        first_core = atoi(optarg);
        break;
      case 'i':
        niters = atoi(optarg);
        break;
      case 't':
        size = atol(optarg);
        break;
      case 'w':
        write_threshold = atof(optarg);
        break;
      case 'd':
        dist_threshold = atof(optarg);
        break;
      case 'b':
        batch_size = atoi(optarg);
        assert(batch_size < MAX_OPS_PER_QUERY);
        break;
      case 'o':
        ops_per_txn = atoi(optarg);
        assert(ops_per_txn < MAX_OPS_PER_QUERY);
        break;
      case 'u':
        track_cpu_usage = 1;
        break;
      default:
        printf("benchmark options are: \n"
               "   -a alpha value for zipf/probability for bernoulli\n"
               "   -h fraction of records to use for hot bernoulli range\n"
               "   -p #servers to use for holding hot bernoulli range\n"
               "   -s number of servers / partitions\n"
               "   -c number of clients\n"
               "   -d ratio of distributed to local txns\n"
               "   -b batch size \n"
               "   -i number of iterations\n"
               "   -o ops per iteration\n"
               "   -t max #records\n"
               "   -m log of max hash key\n"
               "   -w hash insert ratio over total number of queries\n"
               "   -u show server cpu usage\n"
               "example './benchmarkhashtable -d 2 -s 3 -c 3 -f 3 -b 1000 -i 100000000 -t 640000 -m 15 -w 0.3'\n");
        exit(-1);
    }
  }
  if (first_core == -1) first_core = nclients;

  if (alpha) {
    assert(hot_fraction != 0);
    assert(nhot_servers != 0);
    assert(nhot_servers <= nservers);
  }

  // set benchmark to micro for now
  //g_benchmark = &tpcc_bench;
  g_benchmark = &micro_bench;
  run_benchmark();
  return 0;
}

void run_benchmark() 
{
  srand(19890811);
  iters_per_client = niters / nclients;

  printf(" # clients:    %d\n", nclients);
  printf(" # servers:    %d\n", nservers);
  printf(" Key range:    0..2^%d\n", 31-query_shift);
  printf(" Write ratio:  %.3f\n", (double)write_threshold / (double)RAND_MAX);
  printf(" Total #recs: %ld \n", size);
  printf(" Iterations:   %d\n", niters);

  hash_table = create_hash_table(size, nservers);

  start_hash_table_servers(hash_table, first_core);

#if 0
  stats_set_track_cpu_usage(hash_table, track_cpu_usage);
  cdata = malloc(nclients * sizeof(struct client_data));
  for (int i = 0; i < nclients; i++) {
    cdata[i].seed = rand();
    QID[i] = size / nclients * i;
  }
 
  // start the clients
  //ProfilerStart("cpu.info");
  double tstart = now();

  start_hash_table_servers(hash_table, first_core);

  /* insert the data first. 
   * amount of data to insert = cache size
   * number of records to insert (i.e number of iters) = cachesize/recsize 
   */
  write_threshold = (double)RAND_MAX;

  qgen = &get_next_query;
  int r;
  pthread_t *ithreads = (pthread_t *)malloc(nclients * sizeof(pthread_t));
  int *thread_id = (int *)malloc(nclients * sizeof(pthread_t));
  for (int i = 0; i < nclients; i++) {
    thread_id[i] = i;
    r = pthread_create(&ithreads[i], NULL, load_data,
        (void *) &thread_id[i]);
    assert(r == 0);
  }

  void *value;
  for (int i = 0; i < nclients; i++) {
    r = pthread_join(ithreads[i], &value);
    assert(r == 0);
  }

  double tend = now();
  double insert_time = tend - tstart;

  size = stats_get_ninserts(hash_table);
  fprintf(stderr, "loaded %lu pairs.\n", size);

  /* now start lookups */

  stats_reset(hash_table);
  //write_threshold = 0;
  qgen = &get_mixed_query;
  write_threshold = tmp_wt;
  niters = tmp_iters;
  iters_per_client = niters / nclients;

  srand(19890811);
  for (int i = 0; i < nclients; i++) {
    cdata[i].seed = rand();
    //QID[i] = size / nclients * i;
  }

  for (int i = 0; i < nclients; i++) {
    for (int k = 0; k < NEVT; k++) {
      if (StartCounter(i, k, evts[k])) {
        printf("Failed to start counter on cpu %d, "
                "make sure you have run \"modprobe msr\"" 
            " and are running benchmark with sudo privileges\n", i);
      }
      ReadCounter(i, k, &pmclast[k][i]);
    }
  }

  for (int i = first_core; i < first_core + nservers; i++) {
    for (int k = 0; k < NEVT; k++) {
      if (StartCounter(i, k, evts[k])) {
        printf("Failed to start counter on cpu %d, "
                "make sure you have run \"modprobe msr\"" 
            " and are running benchmark with sudo privileges\n", i);
      }

      ReadCounter(i, k, &pmclast[k][i]);
    }
  }

  tstart = now();

  fprintf(stderr, "Starting lookup threads now..\n");
 
  pthread_t *lthreads = (pthread_t *)malloc(nclients * sizeof(pthread_t));
  for (int i = 0; i < nclients; i++) {
    thread_id[i] = i;

    r = pthread_create(&lthreads[i], NULL, run_tests /* ycsb_client */, (void *) &thread_id[i]);
    assert(r == 0);
  }

  printf("waiting for lookup threads now..\n");

  for (int i = 0; i < nclients; i++) {
    r = pthread_join(lthreads[i], &value);
    assert(r == 0);
  }
 
  tend = now();

  stop_hash_table_servers(hash_table);

  double clients_totalpmc[NEVT] = { 0 };
  double servers_totalpmc[NEVT] = { 0 };
  for (int i = 0; i < nclients; i++) {
    for (int k = 0; k < NEVT; k++) {
      uint64_t tmp;
      ReadCounter(i, k, &tmp);
      clients_totalpmc[k] += tmp - pmclast[k][i];
    }
  }
  for (int i = first_core; i < first_core + nservers; i++) {
    for (int k = 0; k < NEVT; k++) {
      uint64_t tmp;
      ReadCounter(i, k, &tmp);
      servers_totalpmc[k] += tmp - pmclast[k][i];
    }
  }

  //ProfilerStop();

  // print out all the important information
  printf("== results ==\n");
  printf(" Loading time:      %.3f\n", insert_time);
  printf(" Lookup time:      %.3f\n", tend - tstart);
  printf(" Lookup hit rate: %.3f(%d,%d)\n", (double)stats_get_nhits(hash_table) / stats_get_nlookups(hash_table), stats_get_nhits(hash_table), stats_get_nlookups(hash_table));
  printf(" Abort rate: %.3f(%d,%d)\n", (double)stats_get_naborts(hash_table) / stats_get_nlookups(hash_table), stats_get_naborts(hash_table), stats_get_nlookups(hash_table));
  printf(" Update ratio: %.3f(%d,%d)\n", (double)stats_get_nupdates(hash_table) / stats_get_nlookups(hash_table), stats_get_nupdates(hash_table), stats_get_nlookups(hash_table));

  printf(" Servr CPU usage: %.3f\n", stats_get_cpu_usage(hash_table));
  if (NEVT > 0) {
    printf(" L2 Misses per iteration: clients - %.3f, servers - %.3f, total - %.3f\n", 
        clients_totalpmc[0] / niters, servers_totalpmc[0] / niters, (clients_totalpmc[0] + servers_totalpmc[0]) / niters);
  }
  if (NEVT > 1) {
    printf(" L3 Misses per iteration: clients - %.3f, servers - %.3f, total - %.3f\n", 
        clients_totalpmc[1] / niters, servers_totalpmc[1] / niters, (clients_totalpmc[1] + servers_totalpmc[1]) / niters);
  }

  free(thread_id);
  free(ithreads);
  free(lthreads);
  free(cdata);
#endif

  printf("== results ==\n");
  printf("Total tps: %0.9fM\n", stats_get_tps(hash_table));
  //printf(" Lookup hit rate: %.3f\n", (double)stats_get_nhits(hash_table) / stats_get_nlookups(hash_table));
#if GATHER_STATS
  stats_get_nlookups(hash_table);
  stats_get_ninserts(hash_table);
  stats_get_nupdates(hash_table);
  stats_get_naborts(hash_table);
#endif
 
  destroy_hash_table(hash_table);
}

#if 0
void get_next_query(int client_id, struct hash_query *query)
{
  enum optype optype = OPTYPE_INSERT;

  unsigned long r = QID[client_id]++;

  query->optype = optype;
  query->key = r & query_mask;
  query->size = YCSB_REC_SZ;
}

void get_mixed_query(int client_id, struct hash_query *query)
{
  enum optype optype =  
    (rand_r(&cdata[client_id].seed) < write_threshold) ? OPTYPE_UPDATE : OPTYPE_LOOKUP; 
  //unsigned long r = QID[client_id]++;
  unsigned long r = (unsigned long) (size * 
    ((double)rand_r(&cdata[client_id].seed) / RAND_MAX));
  assert (r < size);

  query->optype = optype;
  query->key = r & query_mask;
  query->size = 0;
}

void get_random_query(int client_id, struct hash_query *query)
{
  enum optype optype = 
    (rand_r(&cdata[client_id].seed) < write_threshold) ? OPTYPE_INSERT : OPTYPE_LOOKUP; 
  unsigned long r = rand_r(&cdata[client_id].seed);

  query->optype = optype;
  query->key = (r >> query_shift) & query_mask;
  query->size = 0;
  if (optype == OPTYPE_INSERT) {
    //query->size = sizeof(struct ycsb_record);
    query->size = YCSB_REC_SZ;
  }
}

void handle_query_result(int client_id, struct hash_query *query, void *value, int status)
{
  uint64_t *r = (uint64_t *)value;

  if (r == NULL) {
    assert (status == ABORT);
    return;
  }

  if (query->optype == OPTYPE_LOOKUP) {
    if (r != NULL && status == COMMIT) {
      if (*r != query->key) {
        printf("ERROR: values %" PRId64 "-- %d do not match\n", *r, client_id);
        exit(1);
      }

        mp_release_value(hash_table, client_id, value);
    }
  } else if (status == COMMIT) {
    assert(r != NULL);

    if (query->optype == OPTYPE_INSERT)
      *r = query->key;
    else {
      assert(query->optype == OPTYPE_UPDATE);
      if (*r != query->key) {
        printf("UPD ERROR: values %" PRId64 "-- %d do not match\n", *r, client_id);
        exit(1);
      }
    }

    mp_mark_ready(hash_table, client_id, value);
  }
}

int run_transaction(int cid, struct hash_query *ops, int nops)
{
  int i, r;
  void **values = (void **)memalign(CACHELINE, batch_size * sizeof(void *));

  for (i = 0; i < nops; i++)
      values[i] = 0;

  smp_hash_doall(hash_table, cid, nops, ops, values);

  // even if one value is null, we need to abort
  r = COMMIT;
  for (i = 0; i < nops; i++) {
    if (values[i] == NULL) {
      r = ABORT;
      break;
    }
  }

  for (int k = 0; k < nops; k++) {
    handle_query_result(cid, &ops[k], values[k], r);
  }

  free(values);

  return r;
}

void *run_tests(void *args)
{
  int c = *(int *)args;
  set_affinity(c);
    
  int cid = create_hash_table_client(hash_table);
    
  struct hash_query *queries = (struct hash_query *)memalign(CACHELINE, batch_size * sizeof(struct hash_query));
  int i = 0;

  while (i < iters_per_client) {
    int nqueries = min(iters_per_client - i, batch_size);
    for (int k = 0; k < nqueries; k++) {
      //get_random_query(c, &queries[k]);
      (*qgen)(c, &queries[k]);
    }

    while (run_transaction(cid, queries, nqueries) == ABORT)
      ;

    i += nqueries;

    // flush out the buffer so that all remaining releases go out
    smp_flush_all(hash_table, cid);
  }

  free(queries);

  return NULL;
}

void *load_data(void *args)
{
  int c = *(int *)args;
  set_affinity(c);
    
  int cid = create_hash_table_client(hash_table);
    
  struct hash_query *queries = (struct hash_query *)memalign(CACHELINE, batch_size * sizeof(struct hash_query));
  void **values = (void **)memalign(CACHELINE, batch_size * sizeof(void *));
  int i = 0;
  while (i < iters_per_client) {
    int nqueries = min(iters_per_client - i, batch_size);
    for (int k = 0; k < nqueries; k++) {
      //get_random_query(c, &queries[k]);
      (*qgen)(c, &queries[k]);
      values[k] = 0;
    }
    smp_hash_doall(hash_table, cid, nqueries, queries, values);

    for (int k = 0; k < nqueries; k++) {
      handle_query_result(cid, &queries[k], values[k], COMMIT);
    }
    i += nqueries;

    // flush out the buffer so that all remaining releases go out
    smp_flush_all(hash_table, cid);
  }

  free(queries);
  free(values);
  return NULL;
}

#endif
