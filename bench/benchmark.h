#ifndef __BENCHMARK_H_
#define __BENCHMARK_H_

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
#define P_MIX 4 //43
#define OS_MIX 4 //4
#define D_MIX 4 //4
#define SL_MIX 43 //4
#define MIX_COUNT 100
int sequence[MIX_COUNT];

#endif
