#define __MAIN__

#include "headers.h"
#include "benchmark.h"
#include "hashprotocol.h"
#include "smphashtable.h"
#include "partition.h"
#include "master.h"

#define card_ware_house 1
#define card_district 10
#define card_customer 30000
#define card_stock 100000
#define card_item 100000
#define card_order (30000 + 0)
#define card_order_line (300000 + 0)
#define card_new_order (9000 + 0)
#define card_history (30000 + 0)

int queries_per_txn = 1;
int query_mask      = (1 << 29) - 1;
int query_shift     = 2;

int QID[MAX_CLIENTS];

int iters_per_client;

struct client_data {
    unsigned int seed;
} __attribute__ ((aligned (CACHELINE)));
struct client_data *cdata;

void run_benchmark();
void get_random_query(int client_id, struct hash_query *query);
void get_next_query(int client_id, struct hash_query *query);
void get_mixed_query(int client_id, struct hash_query *query);
void (*qgen)(int client_id, struct hash_query *query);

void help()
{
    printf("benchmark options are: \n"
            "-a alpha value for zipf/probability for bernoulli\n"
            "-b type of benchmark (0 = micro, 1 = ycsb, 2 = tpcc)\n"
            "-c number of active servers (used in virt)\n"
            "-d ratio of distributed to local txns\n"
            "-f number of fibers per thread \n"
            "-h fraction of records to use for hot bernoulli range\n"
            "-i number of iterations\n"
            "-o ops per iteration\n"
            "-p #servers to use for holding hot bernoulli range\n"
            "-r nremote operations per txn\n"
            "-s number of servers / partitions\n"
            "-t max #records\n"
            "-v stats verbosity (0/1 log of access counts\n"
            "-w hash insert ratio over total number of queries\n");

    exit(1);
}
void init_seq_array(){

  int total = 0;
  for (int i = 0 ; i < NO_MIX ; ++i){
    sequence[i] = 1;
  }
  total = NO_MIX;
  for(int i = 0 ; i < P_MIX ; ++i){
    sequence[i + total] = 2;
  }
  total = total + P_MIX;
  for(int i = 0 ; i < OS_MIX ; ++i){
    sequence[i+total] = 3;
  }
  total = total + OS_MIX;
  for( int i = 0 ; i < D_MIX ; ++i){
    sequence[i + total] = 4;
  }
  total = total + D_MIX;
  for(int i = 0 ; i < SL_MIX ; ++i){
    sequence[ i + total ] = 5;
  }
  //shuffle elements of the sequence array
  srand ( time(NULL) );
  for (int i = MIX_COUNT-1 ; i > 0; i--)
  {
      int j = rand() % (i+1);
      int temp = sequence[i];
      sequence[i] = sequence[j];
      sequence[j] = temp;
  }
}

int main(int argc, char *argv[])
{
    int opt_char;

    /* defaults */
    g_startup_servers = g_active_servers = 0;
    g_nservers = 1;
    g_nrecs = 1000000;
    g_alpha = 0.8;
    g_niters = 100000000;
    g_nhot_servers = 0;
    g_nhot_recs = 0;
    g_ops_per_txn = 2;
    g_nremote_ops = 2;
    g_write_threshold = 20;
    g_dist_threshold = 100;
    g_nfibers = 1;
    g_verbosity = 0;
    g_benchmark = &micro_bench;

    if (argc < 2)
        help();

    while((opt_char = getopt(argc, argv, "a:s:c:f:i:n:t:m:w:d:f:b:e:u:o:r:h:p:")) != -1) {
        switch (opt_char) {
            case 'a':
                g_alpha = atof(optarg);
                break;
            case 'b':
                switch (atoi(optarg)) {
                    case 1:
                        //XXX: Change this to ycsb
                        g_benchmark = &ycsb_bench;
                        break;
                    case 2:
                        g_benchmark = &tpcc_bench;
                        break;
                }
                break;
            case 'c':
            	g_active_servers = atoi(optarg);
            	g_startup_servers = g_active_servers;
            	assert(g_active_servers < MAX_SERVERS);
            	break;
            case 'd':
                g_dist_threshold = atoi(optarg);
                break;
            case 'f':
                g_nfibers = atoi(optarg);
#if (!defined(MIGRATION) && defined(SHARED_EVERYTHING)) || defined(SHARED_NOTHING)
                if (g_nfibers != 1) {
                    printf("batching not allowed in se/sn modes\n");
                    assert(0);
                }
#endif
                break;
             case 'h':
                g_nhot_recs = atol(optarg);
                break;
            case 'i':
                g_niters = atoi(optarg);
                break;
            case 'o':
                g_ops_per_txn = atoi(optarg);
                assert(g_ops_per_txn < MAX_OPS_PER_QUERY);
                break;
            case 'p':
                g_nhot_servers = atoi(optarg);
                break;
           case 'r':
                g_nremote_ops = atoi(optarg);
                break;
            case 's':
                g_nservers = atoi(optarg);
                assert(g_nservers < MAX_SERVERS && g_nservers <= NCORES);
                break;
            case 't':
                g_nrecs = atol(optarg);
                break;
            case 'v':
                g_verbosity = atoi(optarg);
                break;
            case 'w':
                g_write_threshold = atoi(optarg);
                break;
            default:
                help();
                break;
        }
    }

    // if startup/active servers is not specified, just set it to total
    if (!g_active_servers) {
        g_startup_servers = g_nservers;
        g_active_servers = g_nservers;
    }

#if ENABLE_BWAIT_CC
#if !defined(ENABLE_KEY_SORTING)
#error  "Error. bwait requires key sorting\n"
#endif
#if defined(SHARED_EVERYTHING)
#error "SE doesn't support bwait yet\n"
#endif
#endif

#if ENABLE_ASYMMETRIC_MESSAGING
#if defined(SHARED_EVERYTHING) || defined(SHARED_NOTHING)
#error "Asymmetric messaging valid only in msgpassing mode\n"
#endif
#endif

#if RWTICKET_LOCK
#if defined(ENABLE_WAIT_DIE_CC) || !defined(SHARED_EVERYTHING)
#error "RW ticket lock only works in nowait, shared everything mode\n"
#endif
#endif

#if !defined(SE_LATCH)
#pragma message ( "Not using record latching\n" )
    printf("WARNING: Not using record latching\n");
#endif

#if !defined(SE_INDEX_LATCH)
#pragma message ( "Not using index latching\n" )
    printf("WARNING: Not using index latching\n");
#endif

    printf("%d remote ops %d ops per txn \n", g_nremote_ops, g_ops_per_txn);
    assert(g_nremote_ops <= g_ops_per_txn &&
            g_nremote_ops < MAX_OPS_PER_QUERY);

#if !defined(YCSB_BENCHMARK)
#endif

    // round down nrecs to a partition multiple
    g_nrecs = (g_startup_servers*( card_ware_house + card_district + card_customer + card_order + card_order_line + card_stock + card_new_order + card_history) + card_item) * g_startup_servers;

    assert(g_benchmark);

    run_benchmark();

    return 0;
}

void run_benchmark()
{
    srand(19890811);
    init_seq_array();

    printf(" # servers:    %d\n", g_nservers);
    printf(" Key range:    0..2^%d\n", 31-query_shift);
    printf(" Write ratio:  %d\n", g_write_threshold);
    printf(" Total #recs: %ld \n", g_nrecs);
    printf(" Iterations:   %d\n", g_niters);

    if (g_benchmark->init)
        g_benchmark->init();

#if ENABLE_DL_DETECT_CC
#include "dreadlock_detect.h"
    dreadlock_init();
#endif //ENABLE_DL_DETECT_CC
    hash_table = create_hash_table();

    for(int s = 0 ; s < g_nservers ; ++s){
      
      hash_table->partitions[s].next_Transaction = 0;
    }

    start_hash_table_servers(hash_table);

    printf("== results ==\n");
    printf("Total tps: %0.9fM\n", stats_get_tps(hash_table));
    stats_get_naborts(hash_table);
    stats_get_ncommits(hash_table);
    for (int s = 0; s < g_nservers; s++){
      printf("server %d called new order %d times\n",s,hash_table->partitions[s].new_order_counter);
      printf("server %d called payment %d times\n",s,hash_table->partitions[s].payment_counter);
      printf("server %d called order status %d times\n",s,hash_table->partitions[s].order_status_counter);
      printf("server %d called delivery %d times\n",s,hash_table->partitions[s].delivery_counter);
      printf("server %d called stock level %d times\n",s,hash_table->partitions[s].stock_level_counter);
    }

#if GATHER_STATS
    //stats_get_task_stats(hash_table);
    stats_get_nlookups(hash_table);
    stats_get_ninserts(hash_table);
    stats_get_nupdates(hash_table);
    stats_get_latency(hash_table);

    if (g_verbosity == 1) {
        struct elem *e = (struct elem *) malloc(sizeof(struct elem));
        uint64_t *freqs = (uint64_t *) calloc(g_nrecs, sizeof(uint64_t));

        for (int s = 0; s < g_nservers; s++) {
            printf("Logging srv %d\n", s);
            struct partition *p = &hash_table->partitions[s];
            for (int i = 0; i < p->nhash; i++) {
                struct bucket *b = &p->table[i];
                LIST_FOREACH(e, &b->chain, chain) {
                    uint64_t *int_val = (uint64_t *)e->value;
                    freqs[e->key] = int_val[0];
                }
            }
        }

        FILE *fp = fopen("key_access_freqs.txt", "w");
        if (fp != NULL) {
            for (uint64_t i = 0; i < g_nrecs; i ++) {
                fprintf(fp, "%ld %ld\n", i, freqs[i]);
            }
            fclose(fp);
        } else {
            printf("Could not open file for writing\n");
        }
    }

#endif

    destroy_hash_table(hash_table);
}
