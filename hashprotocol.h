#ifndef __HASHPROTOCOL_H_
#define __HASHPROTOCOL_H_

/**
 * hash operations
 */
enum optype {
  OPTYPE_LOOKUP = 0,
  OPTYPE_INSERT = 1,
  OPTYPE_UPDATE = 2,
  OPTYPE_PLOCK_ACQUIRE = 3,
  OPTYPE_PLOCK_RELEASE = 4,
  OPTYPE_CERTIFY = 5,
};

/**
 * hash_key - Hash table key type
 */
typedef uint64_t hash_key;

struct hash_op {
  uint32_t optype;
  uint32_t size;
  hash_key key;
};

#define MAKE_OP(op,type,sz,k)\
{\
  op.optype = type;\
  op.size = sz;\
  op.key = k;\
}

/**
 * struct hash_query - Hash table query
 * @optype: operation
 * @size: size of data to insert
 * @key: key to lookup or insert
 */
struct hash_query {
  struct hash_op ops[MAX_OPS_PER_QUERY];
  short nops;
  char state;
};

enum query_state {
  HASH_QUERY_EMPTY, 
  HASH_QUERY_COMMITTED, 
  HASH_QUERY_ABORTED
};

#endif
