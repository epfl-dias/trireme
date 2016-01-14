 /* profiling indicates killer to the undo allocations which are a complete 
 * waste if there are no aborts. 
 *
 * Instead of creating a full blown malloc implemenatation, we first fix
 * this abort overhead. for undo data, we call plmalloc. if there are no
 * tuples of that size, we call memalign to get a new tuple. We create a new
 * memtuple structure, store this pointer there, and send back mem_tuple to
 * caller. 
 *
 * When plfree is called with mem_tuple and size, we just link it to the 
 * corresponding linked list in the slab. 
 *
 * When txn_op calls plfree a
 */

#include "headers.h"
#include "plmalloc.h"

void plmalloc_init(struct partition *p)
{
  p->heap.nslabs = 0;
  p->heap.npreallocs = 0;

  for (int i = 0; i < MAX_TUPLE_TYPES; i++) {
    struct mem_bucket *b = &p->heap.slab[i];
    b->tsize = 0;
    LIST_INIT(&b->list);
  }
}

struct mem_tuple *plmalloc_alloc(struct partition *p, size_t size)
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
    m = malloc(sizeof(struct mem_tuple));
    //m->data = memalign(CACHELINE, size);
    m->data = malloc(size);
  }

  return m;
}

void plmalloc_free(struct partition *p, struct mem_tuple *m, size_t size)
{
  int nslabs = p->heap.nslabs;
  struct mem_bucket *b = NULL;
  int i;

  // search for the slab
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

  assert(b);

  // add memtuple to the target slab
  LIST_INSERT_HEAD(&b->list, m, next);
}

void plmalloc_destroy(struct partition *p)
{
  for (int i = 0; i < MAX_TUPLE_TYPES; i++) {
    struct tlist *t = &p->heap.slab[i].list;
    struct mem_tuple *m = LIST_FIRST(t);

    while (m) {
      struct mem_tuple *next = LIST_NEXT(m, next);
      free(m->data);
      free(m);
      m = next;
    }
  }
}
  
