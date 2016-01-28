#ifndef __CONST_H__
#define __CONST_H__

#define TXN_BATCH 1
#define TXN_SINGLE 0

#define DATA_READY_MASK 0x8000000000000000

#define FALSE 0
#define TRUE 1
#define TXN_COMMIT 0
#define TXN_ABORT 1

#define CACHELINE 64 
#define MAX_CLIENTS 128 // must be power of 2
#ifndef MAX_SERVERS
#define MAX_SERVERS 128 // must be power of 2
#endif

#define YCSB_NFIELDS 1
#define YCSB_FIELD_SZ 8
#define YCSB_REC_SZ (YCSB_NFIELDS * YCSB_FIELD_SZ)

/* commn. buffer constants */
#define ONEWAY_BUFFER_SIZE  (16 * (CACHELINE >> 3)) 
#define BUFFER_FLUSH_COUNT  8

#define MAX_OPS_PER_QUERY 64

#define MAX_TUPLE_TYPES 32

#if defined(SHARED_EVERYTHING) || defined(SHARED_NOTHING)
// by default, disable socket local for shared everything/nothing
#else
#define ENABLE_SOCKET_LOCAL_TXN 1
#endif

#define LOCK_SUCCESS 0
#define LOCK_ABORT 1
#define LOCK_WAIT 2

#define NREMOTE_OPS 2

#endif
