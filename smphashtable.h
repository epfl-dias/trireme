#ifndef __SMPHASHTABLE_H_
#define __SMPHASHTABLE_H_

#include "hashprotocol.h"

struct hash_table *create_hash_table();
void destroy_hash_table(struct hash_table *hash_table);
//void start_hash_table_servers(struct hash_table *hash_table);
void stop_hash_table_servers(struct hash_table *hash_table);
void create_hash_table_client(struct hash_table *hash_table);

void smp_flush_all(struct hash_table *hash_table, int client_id);
int smp_hash_update(struct task *ctask, struct hash_table *hash_table, 
    int client_id, int server, hash_key key, short op_id);
void smp_hash_doall(struct task *ctask, struct hash_table *hash_table,
    int client_id, int s, int nqueries, struct hash_op **queries,
    void **values, int opid);

void mp_mark_ready(struct hash_table *hash_table, int client_id, int target, int tid,
        int opid, void *ptr, char optype);
void mp_release_plock(int s, int c);
void mp_release_value_(struct partition *p, struct elem *e);
void mp_send_reply(int s, int c, short task_id, short opid, struct elem *e);

/**
 * Stats functions
 */
int stats_get_task_stats(struct hash_table *hash_table);
void stats_reset(struct hash_table *hash_table);
int stats_get_ncommits(struct hash_table *hash_table);
int stats_get_nlookups(struct hash_table *hash_table);
int stats_get_nupdates(struct hash_table *hash_table);
int stats_get_naborts(struct hash_table *hash_table);
int stats_get_ninserts(struct hash_table *hash_table);
void stats_get_buckets(struct hash_table *hash_table, int server, double *avg, double *stddev);
void stats_get_mem(struct hash_table *hash_table, size_t *used, size_t *total);
double stats_get_tps(struct hash_table *hash_table);
int stats_get_latency(struct hash_table *hash_table);

/* txn functions */
void txn_start(struct hash_table *hash_table, int s, struct txn_ctx *ctx);
int txn_commit(struct task *t, struct hash_table *hash_table, int s, int mode);
void txn_abort(struct task *t, struct hash_table *hash_table, int s, int mode);
int txn_finish(struct task *ctask, struct hash_table *hash_table, int s, 
    int status, int mode, short *opids);
void *txn_op(struct task *t, struct hash_table *hash_table, int s, 
    struct hash_op *op, int target);
int hash_get_server(const struct hash_table *hash_table, hash_key key);

/* misc */
void process_requests(struct hash_table *hash_table, int s);
int is_value_ready(struct elem *e);
int run_batch_txn(struct hash_table *hash_table, int s, void *arg, 
    struct task *t);

#endif
