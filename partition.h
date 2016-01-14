#ifndef __PARTITION_H_
#define __PARTITION_H_

#include <sys/queue.h>

#include "hashprotocol.h"
#include "util.h"
#include "pthread.h"
#include "selock.h"

typedef void release_value_f(struct elem *e);

void init_hash_partition(struct partition *p, size_t max_size, int nservers, 
    char alloc);

size_t destroy_hash_partition(struct partition *p);

struct elem * hash_lookup(struct partition *p, hash_key key);
struct elem * hash_insert(struct partition *p, hash_key key, int size, release_value_f *release);
int hash_get_bucket(const struct partition *p, hash_key key);
void hash_remove(struct partition *p, struct elem *e);

#endif
