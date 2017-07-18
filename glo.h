#pragma once

#if defined(__MAIN__)
#define EXTERN
#else
#define EXTERN extern
#endif

EXTERN int g_nservers;
EXTERN int g_nrecs;
EXTERN struct benchmark *g_benchmark;
EXTERN struct hash_table *hash_table;
EXTERN double g_alpha;
EXTERN int g_niters;
EXTERN int g_nhot_servers;
EXTERN int g_nhot_recs;
EXTERN int g_ops_per_txn;
EXTERN int g_nremote_ops;
EXTERN int g_dist_threshold;
EXTERN int g_write_threshold;
EXTERN int g_batch_size;
EXTERN int g_verbosity;
EXTERN volatile int nready;

EXTERN struct task* g_tasks[144][16];

#if ENABLE_DL_DETECT_CC
#include "dl_detect.h"
EXTERN DL_detect dl_detector;
#endif

#if YCSB_BENCHMARK
  EXTERN struct drand48_data *rand_buffer;
  EXTERN double g_zetan;
  EXTERN double g_zeta2;
  EXTERN double g_eta;
  EXTERN double g_alpha_half_pow;
#endif

