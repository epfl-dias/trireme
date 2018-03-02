#ifndef __BENCHMARK_H_
#define __BENCHMARK_H_

void init_tpcc_seq_array();
void print_tpcc_mix(int g_nservers, struct hash_table *hash_table);
void count_tpcc_transaction(struct hash_table *hash_table, int s, int tnx);

struct benchmark {
  void (*init)();
  void (*load_data)(struct hash_table *h, int s);
  void *(*alloc_query)();
  void (*get_next_query)(struct hash_table *h, int s, void *q);
  int (*run_txn)(struct hash_table *hash_table, int s, void *arg,
      struct task *t);
  void (*verify_txn)(struct hash_table *hash_table, int s);
};

extern struct benchmark tpcc_bench;
extern struct benchmark micro_bench;
extern struct benchmark ycsb_bench;
#define NO_MIX 45 //45
#define P_MIX  43//43
#define OS_MIX 4 //4
#define D_MIX 4 //4
#define SL_MIX 4 //4
#define MIX_COUNT 100
#define card_ware_house 1
#define card_district 10
#define card_customer 30000
#define card_stock 100000
#define card_item 100000
#define card_order (30000 + 10000)
#define card_order_line (300000 + 10000)
#define card_new_order (9000 + 3000)
#define card_history (30000 + 10000)
int sequence[MIX_COUNT];
int tpcc_flag;

#endif
