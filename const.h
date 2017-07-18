#ifndef __CONST_H__
#define __CONST_H__

#if DIASSRV8
#define NCORES 80
#define NSOCKETS 8
#elif DIASCLD33
#define NSOCKETS 4
#if HT_ENABLED
#define NCORES 144
#else
#define NCORES 72
#endif
#else
#define NCORES 4
#define NSOCKETS 1
#endif

#define TXN_BATCH 1
#define TXN_SINGLE 0

#define USE_PHYS_SYNCH 1

#define DATA_READY_MASK         0x8000000000000000

#define CL_SIZE	64 // constant for dl_detect

#define FALSE 0
#define TRUE 1
#define TXN_COMMIT 0
#define TXN_ABORT 1
#define ABORT_PENALTY 100000 // same as dbx1000

#define CACHELINE 64 
#define MAX_CLIENTS 128 // must be power of 2
#ifndef MAX_SERVERS
#define MAX_SERVERS 128 // must be power of 2
#endif

#if YCSB_BENCHMARK
#define YCSB_NFIELDS 10
#define YCSB_FIELD_SZ 100
#else
#define YCSB_NFIELDS 10
#define YCSB_FIELD_SZ 8
#endif
#define YCSB_REC_SZ (YCSB_NFIELDS * YCSB_FIELD_SZ)

/* commn. buffer constants */
#if ENABLE_DL_DETECT_CC
#define ONEWAY_BUFFER_SIZE  (1024 * (CACHELINE >> 3))
#else
#define ONEWAY_BUFFER_SIZE  (32 * (CACHELINE >> 3))
#endif // ENABLE_DL_DETECT_CC

#define BUFFER_FLUSH_COUNT  8

/* max ops per query is limited by hashop_opid_mask */
#define MAX_OPS_PER_QUERY 255

#define NQUERIES_PER_TASK 1

#define MAX_TUPLE_TYPES 128

#define RUN_TIME 10000000

#if defined(SHARED_EVERYTHING) || defined(SHARED_NOTHING)
// by default, disable socket local for shared everything/nothing
#else
//#define ENABLE_SOCKET_LOCAL_TXN 1
#endif

#define LATCH_SUCCESS 0
#define LATCH_FAILURE 1

#define LOCK_SUCCESS 0
#define LOCK_ABORT 1
#define LOCK_WAIT 2
#define LOCK_INVALID 3
#define LOCK_ABORT_NXT 4

/** 
 * Hash Table Operations
 */
/* exploit the fact that we have 48-bit address space and use the upper
 * 16-bits to encode operation type and msg tag
 */
#define HASHOP_MASK           0xF000000000000000
#define HASHOP_TID_MASK       0x0F00000000000000
#define HASHOP_OPID_MASK      0x00FF000000000000
#define MAKE_HASH_MSG(tid,opid,key,optype)\
  ((optype) | ((unsigned long)(tid) << 56UL) | ((unsigned long)opid << 48UL) | (key))

#define HASHOP_GET_TID(msg)   (((msg) & HASHOP_TID_MASK) >> 56)
#define HASHOP_GET_OPID(msg)   (((msg) & HASHOP_OPID_MASK) >> 48)
#define HASHOP_GET_VAL(msg)   ((msg) & ~HASHOP_TID_MASK & ~HASHOP_MASK & ~HASHOP_OPID_MASK)

#if ENABLE_DL_DETECT_CC
#define HASHOP_TSMSG_OPID_MASK	0xFF00000000000000
#define HASHOP_TSMSG_TS_MASK	0x00FFFFFFFFFFFFFF

#define MAKE_TS_MSG(opid,ts)\
	((unsigned long)(opid) << 56UL) | (ts >> 8)

#define HASHOP_TSMSG_GET_OPID(msg)   (((msg) & HASHOP_TSMSG_OPID_MASK) >> 56)
#define HASHOP_TSMSG_GET_TS(msg)   ((msg) & HASHOP_TSMSG_TS_MASK)

#endif

#define HASHOP_LOOKUP         0x1000000000000000 
#define HASHOP_INSERT         0x2000000000000000 
#define HASHOP_UPDATE         0x3000000000000000 
#define HASHOP_RELEASE        0x4000000000000000 
#define HASHOP_PLOCK_ACQUIRE  0x5000000000000000 
#define HASHOP_PLOCK_RELEASE  0x6000000000000000
#define HASHOP_MIGRATE        0x7000000000000000
#define HASHOP_RD_RELEASE     0x8000000000000000
#define HASHOP_WT_RELEASE     0x9000000000000000
#define HASHOP_CERTIFY        0xA000000000000000
#define HASHOP_CERT_RELEASE   0xB000000000000000

#if ENABLE_DL_DETECT_CC
#define DL_DETECT_ADD_DEP_SRC	  0x7000000000000000
#define DL_DETECT_ADD_DEP_TRG	  0x8000000000000000
#define DL_DETECT_RMV_DEP_SRC	  0x9000000000000000
#define DL_DETECT_RMV_DEP_TRG	  0xA000000000000000
#define DL_DETECT_CLR_DEP	      0xB000000000000000
#define DL_DETECT_ABT_TXN         0xC000000000000000
#endif

#define RELEASE_MSG_LENGTH 1
#define MIGRATE_MSG_LENGTH 1

#if ENABLE_WAIT_DIE_CC || ENABLE_DL_DETECT_CC
#define INSERT_MSG_LENGTH 3
#define LOOKUP_MSG_LENGTH 2
#else
#define INSERT_MSG_LENGTH 2
#define LOOKUP_MSG_LENGTH 1
#endif

#endif
