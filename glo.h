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

#if ENABLE_DL_DETECT_CC
EXTERN DL_detect dl_detector;
#endif

