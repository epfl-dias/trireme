#include "headers.h"
#include "smphashtable.h"
#include "plmalloc.h"

#if ENABLE_MV2PL

int set_read_lock(volatile int64_t *val, char use_phys_synch)
{
    int64_t old, new;
    old = *val;

    while (1) {
        if (old == -1)
            return LOCK_ABORT;

        if (use_phys_synch) {
            new = __sync_val_compare_and_swap(val, old, old + 1);
        } else {
            new = old;
            *val = old + 1;
        }

        if (new == old)
            break;

        assert(use_phys_synch);
        old = new;
    }

    assert(old == new && *val != -1);

    return LOCK_SUCCESS;
}

void clear_lock(char optype, struct elem *e, char is_certified)
{
    switch(optype) {
        case OPTYPE_LOOKUP:
            assert(e->rd_counter > 0);

            int64_t old_val = __sync_fetch_and_sub(&e->rd_counter, 1);
            assert(old_val >= 0);
            break;

        case OPTYPE_UPDATE:
            if (is_certified) {
                assert (e->rd_counter == -1);
                e->rd_counter = 0;
            }

            assert(e->is_write_locked);
            e->is_write_locked = 0;

            break;
    }
}

int certify_writes(struct elem *e)
{
  assert(e->is_write_locked);

  int64_t val = e->rd_counter;
  if (val > 0)
      return LOCK_ABORT;

  return __sync_bool_compare_and_swap(&e->rd_counter, val, -1) ?
      LOCK_SUCCESS : LOCK_ABORT;
}

struct elem *mv2pl_acquire(struct partition *p, struct elem *e, char optype)
{
  struct elem *target = NULL;
  int s = p - &hash_table->partitions[0];
  int g_tid = p->current_task->g_tid;

  dprint("srv(%d): %s lock request for key %"PRIu64"\n", s,
      optype == OPTYPE_LOOKUP ? "lookup":"update", e->key);

  if (optype == OPTYPE_LOOKUP) {
      if (set_read_lock(&e->rd_counter, USE_PHYS_SYNCH) == LOCK_SUCCESS)
          target = e;
  } else {
      assert(optype == OPTYPE_UPDATE);

      if (__sync_bool_compare_and_swap(&e->is_write_locked, 0, 1)) {
          target = e;
      }
  }

  return target;
}

void mv2pl_abort(struct task *ctask, struct hash_table *hash_table, int s)
{
  struct txn_ctx *ctx = &ctask->txn_ctx;
  struct partition *p = &hash_table->partitions[s];
  int nops = ctx->nops;

  for (int i = 0; i < nops; i++) {
      struct op_ctx *octx = &ctx->op_ctx[i];

      clear_lock(octx->optype, octx->e, /* is_certified = 0 */ 0);
  }
}

int mv2pl_validate(struct task *ctask, struct hash_table *hash_table, int s)
{
  /* latch, reset ref count to free the logical lock, unlatch */
  struct txn_ctx *ctx = &ctask->txn_ctx;
  struct partition *p = &hash_table->partitions[s];
  int nops = ctx->nops;
  int r = LOCK_SUCCESS;
  int last_certified_wt = -1;
 
  /* go through operations validating reads and acquiring certify locks */
  for (int i = 0; i < nops; i++) {
      struct op_ctx *octx = &ctx->op_ctx[i];
      if (octx->optype == OPTYPE_UPDATE) {
          if ((r = certify_writes(ctx->op_ctx[i].e)) != LOCK_SUCCESS)
              break;

          last_certified_wt = i;
      }
  }

  for (int i = 0; i < nops; i++) {
      struct op_ctx *octx = &ctx->op_ctx[i];

      /* if we have all certify locks, update data */
      if (r == LOCK_SUCCESS && octx->optype == OPTYPE_UPDATE)
          memcpy(octx->e->value, octx->data_copy->value, octx->e->size);

      /* release lock */
      clear_lock(octx->optype, octx->e, i <= last_certified_wt);
  }

  if (r == LOCK_SUCCESS) {
      dprint("srv(%d):mv2pl validation success\n", s);
      p->nvalidate_success++;
      return TXN_COMMIT;
  }

  dprint("srv(%d):mv2pl validation failed\n", s);
  p->nvalidate_failure++;
  return TXN_ABORT;
}

#endif //ENABLE_MV2PL
