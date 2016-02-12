#ifndef __BENCHMARK_H_
#define __BENCHMARK_H_

struct benchmark {
  void (*load_data)(struct hash_table *h, int s);
  void *(*alloc_query)();
  void (*get_next_query)(struct hash_table *h, int s, void *q);
  int (*run_txn)(struct hash_table *hash_table, int s, void *arg, 
      struct task *t, int status);
  void (*verify_txn)(struct hash_table *hash_table, int s);
};

extern struct benchmark tpcc_bench;
extern struct benchmark micro_bench;

#endif


