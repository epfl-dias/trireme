#ifndef __CONST_H__
#define __CONST_H__

#define TXN_BATCH 1
#define TXN_SINGLE 0

#define DATA_READY_MASK 0x8000000000000000

#define FALSE 0
#define TRUE 1
#define TXN_COMMIT 0
#define TXN_ABORT 1

#define CACHELINE   64 
#define MAX_CLIENTS 128 // must be power of 2
#define MAX_SERVERS 128 // must be power of 2

#define YCSB_NFIELDS 1
#define YCSB_FIELD_SZ 8
#define YCSB_REC_SZ (YCSB_NFIELDS * YCSB_FIELD_SZ)

/* commn. buffer constants */
#define ONEWAY_BUFFER_SIZE  (16 * (CACHELINE >> 3)) 
#define BUFFER_FLUSH_COUNT  8

#define MAX_OPS_PER_QUERY 512

#endif
