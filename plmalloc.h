#pragma once

void plmalloc_init(struct partition *p);
struct mem_tuple *plmalloc_alloc(struct partition *p, size_t size);
void plmalloc_free(struct partition *p, struct mem_tuple *ptr, size_t size);
void plmalloc_destroy(struct partition *p);
