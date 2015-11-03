#ifndef __HASHPROTOCOL_H_
#define __HASHPROTOCOL_H_

#include <stdint.h>

/**
 * hash_key - Hash table key type
 */
typedef uint64_t hash_key;

/**
 * hash operations
 */
enum optype {
  OPTYPE_LOOKUP = 0,
  OPTYPE_INSERT = 1,
  OPTYPE_UPDATE = 2
};

struct hash_op {
  uint32_t optype;
  uint32_t size;
  hash_key key;
};

/**
 * struct hash_query - Hash table query
 * @optype: operation
 * @size: size of data to insert
 * @key: key to lookup or insert
 */
struct hash_query {
#define MAX_OPS_PER_QUERY 512
  struct hash_op ops[MAX_OPS_PER_QUERY];
  int nops;
};

#endif
