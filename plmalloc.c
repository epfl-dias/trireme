 /* 
  * Partition Local Malloc (PLMALLOC)
  * Memory for struct elems is managed independently of memory for records as
  * struct elems are small and it is a waste to wrap them with mem_tuples.
  */

#include "headers.h"
#include "plmalloc.h"

static struct mem_bucket *get_slab(struct partition *p, size_t size)
{
  struct mem_bucket *b;
  int i;
  int nslabs = p->heap.nslabs;

  for (i = 0; i < nslabs; i++) {
    b = &p->heap.slab[i];
    if (b->tsize == size)
      break;
  }

  if (i == nslabs) {
    // slab not found. claim a new slab
    assert(nslabs < MAX_TUPLE_TYPES);
    b = &p->heap.slab[nslabs];
    p->heap.nslabs++;

    b->tsize = size;
    LIST_INIT(&b->list);
  }

  return b;
}

static struct mem_tuple *plmalloc_prealloc(struct partition *p, size_t size)
{
  struct mem_bucket *b = get_slab(p, size); 
  int hsize = sizeof(struct mem_tuple);
  int tsize = hsize + size;
  int prealloc_cnt = b->prealloc_cnt * 2;
  prealloc_cnt = prealloc_cnt > 1048576 ? 1048576 : prealloc_cnt;

  // allocate space for all memtuples + one extra for storing the base ptr
  char *buf = malloc(tsize * prealloc_cnt + sizeof(struct mem_tuple));
  assert(buf);

  // first memtuple is used to store base ptr
  struct mem_tuple *bp = (struct mem_tuple *)buf;
  bp->data = buf;
  LIST_INSERT_HEAD(&p->heap.base_ptrs, bp, next);

  // now, form mem_tuples out of the remainder and add them to slabs
  char *idx = buf + hsize;
  for (int i = 0; i < prealloc_cnt; i++) {
    struct mem_tuple *t = (struct mem_tuple *)idx;
    t->data = (char *)t + hsize;
    LIST_INSERT_HEAD(&b->list, t, next);
    idx += tsize;
  }

  b->prealloc_cnt = prealloc_cnt;

  return (struct mem_tuple *)(buf + hsize);
}

void plmalloc_init(struct partition *p)
{
  p->heap.nslabs = 0;

  for (int i = 0; i < MAX_TUPLE_TYPES; i++) {
    struct mem_bucket *b = &p->heap.slab[i];
    b->tsize = 0;
    b->prealloc_cnt = 16;
    LIST_INIT(&b->list);
  }

  LIST_INIT(&p->heap.base_ptrs);
  TAILQ_INIT(&p->heap.efree_list);
}

//struct mem_tuple *plmalloc_alloc(struct partition *p, size_t size)
void *plmalloc_alloc(struct partition *p, size_t size)
{
  int nslabs = p->heap.nslabs;
  struct mem_bucket *b = NULL;
  struct mem_tuple *m = NULL;

  assert(size);

  // search buckets
  for (int i = 0; i < nslabs; i++) {
    b = &p->heap.slab[i];
    if (b->tsize == size) {
      m = LIST_FIRST(&b->list);
      break;
    }
  }

 if (m) {
   LIST_REMOVE(m, next);
 } else {
    m = plmalloc_prealloc(p, size);
    int hsize = sizeof(struct mem_tuple);
    m = malloc(hsize + size);
    //m->data = memalign(CACHELINE, size);
    m->data = (char *)m + hsize; 
  }

  return m->data;
}

void plmalloc_free(struct partition *p, void *ptr, size_t size)
{
  struct mem_bucket *b = NULL;
  struct mem_tuple *m = 
    (struct mem_tuple *)((char *)ptr - sizeof(struct mem_tuple));
  int i;

  // search for the slab
  b = get_slab(p, size);
  assert(b);

  // add memtuple to the target slab
  LIST_INSERT_HEAD(&b->list, m, next);
}

void plmalloc_destroy(struct partition *p)
{
  // just go through the list of base ptrs and free them
  struct mem_tuple *bp = LIST_FIRST(&p->heap.base_ptrs);
  while (bp) {
    struct mem_tuple *next = LIST_NEXT(bp, next);
    free(bp->data);
    bp = next;
  }
  /*
  for (int i = 0; i < MAX_TUPLE_TYPES; i++) {
    struct tlist *t = &p->heap.slab[i].list;
    struct mem_tuple *m = LIST_FIRST(t);

    while (m) {
      struct mem_tuple *next = LIST_NEXT(m, next);
      //free(m->data);
      free(m);
      m = next;
    }
  }
  */
}

struct elem *plmalloc_ealloc(struct partition *p)
{

  struct elem *e;
  struct elist *free_list = &p->heap.efree_list;
  
  if (TAILQ_EMPTY(free_list)) {
    /* prealloc a chunk of elem structs and add them to the free list */
    int esize = 
      ((sizeof(struct elem) + CACHELINE - 1) / CACHELINE) * CACHELINE;

    /* we are preallocating a huge number here = p->nrecs. this will
     * inflat memory in case we don't use that many elem structs
     */
    char *buf = memalign(CACHELINE, esize * p->nrecs);
    assert(buf);

    // add buffer to base pointer list
    struct mem_tuple *bp = malloc(sizeof(struct mem_tuple));
    bp->data = buf;
    LIST_INSERT_HEAD(&p->heap.base_ptrs, bp, next);

    // now make elem structs and add them to the free list
    char *idx = buf;
    for (int i = 0; i < p->nrecs; i++) {
      struct elem *e = (struct elem *)idx;
      TAILQ_INSERT_HEAD(free_list, e, chain);
      idx += esize;
    }
  }

  e = TAILQ_FIRST(free_list);
  assert(e);
  TAILQ_REMOVE(free_list, e, chain);

  return e;
}

void plmalloc_efree(struct partition *p, struct elem *e)
{
  TAILQ_INSERT_HEAD(&p->heap.efree_list, e, chain);
}
