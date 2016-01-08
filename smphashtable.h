#ifndef __SMPHASHTABLE_H_
#define __SMPHASHTABLE_H_

#include "hashprotocol.h"
#include "util.h"
#include "onewaybuffer.h"

#define TXN_BATCH 1
#define TXN_SINGLE 0

#define DATA_READY_MASK 0x8000000000000000

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
  struct partition *g_partition; /* used for item table */
  struct box_array *boxes;
  uint64_t *keys;

  // stats
  int track_cpu_usage;
};


/**
 * create_hash_table - Create new smp hash table
 * @max_size: maximum size in bytes that hash table can occupy
 * @nservers: number of servers that serve hash content
 * @return: pointer to the created hash table
 */
struct hash_table *create_hash_table(size_t max_size, int nservers);

/**
 * destroy_hash_table - Destroy smp hash table
 * @hash_table: pointer to the hash table structure
 */
void destroy_hash_table(struct hash_table *hash_table);

/*
 * start_hash_table_servers - Start up hash table server threads
 * @hash_table: pointer to the hash table structure
 * @first_core: specifies what cores to run servers on [first_core..firt_core+nservers-1]
 *
 * start_hash_table_servers and stop_hash_table_servers must be called from the
 * same thread.
 */
void start_hash_table_servers(struct hash_table *hash_table, int first_core);

/*
 * stop_hash_table_servers - Stop hash table server threads
 * @hash_table: pointer to the hash table structure
 */
void stop_hash_table_servers(struct hash_table *hash_table);

/*
 * create_hash_table_client - Create client to perform hash table operations
 * @hash_table: pointer to the hash table structure
 * @return: created client id
 */
void create_hash_table_client(struct hash_table *hash_table);

/**
 * smp_hash_lookup: Lookup key/value pair in hash table
 * @hash_table: pointer to the hash table structure
 * @client_id: client id to use to communicate with hash table servers
 * @key: hash key to lookup value for
 * @return: 1 for success, 0 on failure when the queue of pending requests is full
 */ 
int smp_hash_lookup(struct hash_table *hash_table, int client_id, hash_key key);

/**
 * smp_hash_insert: Insert key/value pair in hash table
 * @hash_table: pointer to the hash table structure
 * @client_id: client id to use to communicate with hash table servers
 * @key: hash key
 * @size: size of data to insert
 * @return: 1 for success, 0 on failure when the queue of pending requests is full
 */
int smp_hash_insert(struct hash_table *hash_table, int client_id, hash_key key, int size);

/**
 * smp_hash_update: Update key/value pair in hash table
 * @hash_table: pointer to the hash table structure
 * @client_id: client id to use to communicate with hash table servers
 * @key: hash key
 * @size: size of data to insert
 * @return: 1 for success, 0 on failure when the queue of pending requests is full
 */
int smp_hash_update(struct hash_table *hash_table, int client_id, hash_key key);

/**
 * smp_hash_doall: Perform batch hash table queries
 * @hash_table: pointer to the hash table structure
 * @client_id: client id to use to communicate with hash table servers
 * @nqueries: number of queries
 * @queries: hash table quries
 * @values: array to return results of queries
 *
 * NOTE: this has become basically obsolete, it can be substituted by using 
 * smp_hash_lookup, smp_hash_insert, AND get_next or try_get_next
 */
void smp_hash_doall(struct hash_table *hash_table, int client_id, int nqueries, struct hash_op **queries, void **values);


void smp_flush_all(struct hash_table *hash_table, int client_id);

/**
 * mp_release_value, mp_mark_ready: Release given value pointer or mark it ready
 * using message passing. Only works in server/client version
 */
void mp_release_value(struct hash_table *hash_table, int client_id, void *ptr);
void mp_mark_ready(struct hash_table *hash_table, int client_id, void *ptr);


/* is_value_ready check ref counts to see if there is a write lock held
 * on a data item. write lock can be held during insertion or during update
 * During both times, no other read or write is allowed. 
 * This is used by both messaging and shared everything designs
 */
int is_value_ready(struct elem *e);

/**
 * Stats functions
 */
void stats_reset(struct hash_table *hash_table);
int stats_get_nhits(struct hash_table *hash_table);
int stats_get_nlookups(struct hash_table *hash_table);
int stats_get_nupdates(struct hash_table *hash_table);
int stats_get_naborts(struct hash_table *hash_table);
int stats_get_ninserts(struct hash_table *hash_table);
void stats_get_buckets(struct hash_table *hash_table, int server, double *avg, double *stddev);
void stats_get_mem(struct hash_table *hash_table, size_t *used, size_t *total);
void stats_set_track_cpu_usage(struct hash_table *hash_table, int track_cpu_usage);
double stats_get_cpu_usage(struct hash_table *hash_table);
double stats_get_tps(struct hash_table *hash_table);


void txn_start(struct hash_table *hash_table, int s);
void txn_commit(struct hash_table *hash_table, int s, int mode);
void txn_abort(struct hash_table *hash_table, int s, int mode);
void *txn_op(struct hash_table *hash_table, int s, struct partition *p, 
    struct hash_op *op, int is_local);
int hash_get_server(const struct hash_table *hash_table, hash_key key);

#endif
