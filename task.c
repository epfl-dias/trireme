#include "headers.h"
#include "smphashtable.h"
#include "onewaybuffer.h"
#include "benchmark.h"

extern int batch_size;
extern int niters;
extern struct hash_table *hash_table;
extern struct benchmark *g_benchmark;

int task_create(struct partition *p);
int task_join(struct task *root_task);
void task_destroy(struct task *t, struct partition *p);
void task_unblock(struct task *t);

void lightweight_swapcontext(ucontext_t *out, ucontext_t *in);

static struct task *get_task(struct partition *p, int tid)
{
  // only called from unblock_fn. so task should only be inwaiting list
  struct task *t = TAILQ_FIRST(&p->wait_list);
  while (t) {
    if (t->tid == tid)
      break;

    t = TAILQ_NEXT(t, next);
  }

  return t;
}

void unblock_fn(int s, int tid)
{
  struct partition *p = &hash_table->partitions[s];
  struct task *self = &p->unblock_task;
  struct box_array *boxes = hash_table->boxes;
  struct task *t;

  while (1) {

    dprint("srv(%d): Unblock task looping again\n", s);

    // we're done if waiters and ready list is empty
    if (p->q_idx == niters && TAILQ_EMPTY(&p->wait_list) && 
        TAILQ_EMPTY(&p->ready_list)) {
      printf("srv(%d): Unblock task killing self\n", s);
      task_destroy(self, p);
    }

    /* if we have empty waiter list and empty ready list, we are still
     * bootstrapping. So we just have to wait
     */
    if (TAILQ_EMPTY(&p->wait_list) && TAILQ_EMPTY(&p->ready_list)) {
      dprint("srv(%d): Unblock task waiting for waiters\n", s);
      task_yield(p, TASK_STATE_READY);
    }

    // if anybody is ready, yield now
    if (!TAILQ_EMPTY(&p->ready_list)) {
      dprint("srv(%d): Unblock task yielding to someone on ready list\n", s);
      task_yield(p, TASK_STATE_READY);
    }

    process_requests(hash_table, s);

    //smp_flush_all(hash_table, s);

    int nservers = hash_table->nservers;
    for (int i = 0; i < nservers; i++) {
      struct onewaybuffer *b = &boxes[s].boxes[i].out;
      int count = b->wr_index - b->rd_index;

      if (count) {
        uint64_t data[ONEWAY_BUFFER_SIZE];
        count = buffer_read_all(b, ONEWAY_BUFFER_SIZE, data, 0);
        assert(count);

        dprint("srv(%d): got %d msges from %d\n", s, count, i);

        for (int j = 0; j < count; j++) {
          int tid = HASHOP_GET_TID(data[j]);
          struct task *t = get_task(p, tid);
          assert(t && t->state == TASK_STATE_WAITING);

          t->received_responses[t->nresponses] = data[j];
          t->nresponses++;
          assert (t->nresponses <= t->npending);

          // task is ready if we have received all pending responses
          if (t->nresponses == t->npending) {
            dprint("srv(%d): unblocking %d (nresp %d, npend %d)\n", s, t->tid,
                t->nresponses, t->npending);

            task_unblock(t);
          }
        }
      }
    }
  }
}

void child_fn(int s, int tid)
{
  int r;
  struct partition *p = &hash_table->partitions[s];
  struct task *self = p->current_task;
  assert(self && self->s == s);

  struct hash_query *query = self->query;

  while (1) {
    uint64_t q_idx = p->q_idx;

    if (q_idx == niters)
      break;

    g_benchmark->get_next_query(hash_table, s, query);

    dprint("srv(%d): task %d issuing txn %d \n", s, self->tid, q_idx);

    r = TXN_COMMIT;

    do {
#if ENABLE_OP_BATCHING
      r = run_batch_txn(hash_table, self->s, query, self, r);
#else
      r = g_benchmark->run_txn(hash_table, self->s, query, self, r);
#endif
      if (r == TXN_ABORT) {
        dprint("srv(%d):txn %d aborted\n", s, q_idx);
        task_yield(p, TASK_STATE_READY);
        dprint("srv(%d): rerunning aborted txn %d\n", s, q_idx);
      }

    } while (r == TXN_ABORT);

    assert(q_idx <= niters);

    // After each txn, call process request
    process_requests(hash_table, s);

#if PRINT_PROGRESS
    if (q_idx % 100 == 0) {
      printf("srv(%d): task %d finished %s(%" PRId64 " of %d)"
          " on key %" PRId64 "\n", 
          s, 
          self->tid, 
          query->ops[0].optype == OPTYPE_LOOKUP ? "lookup":"update", 
          q_idx, niters,
          query->ops[0].key);
    }

#endif

  }

  free(query);

  task_destroy(self, p);
}

void root_fn(int s, int tid)
{
  int i, r;
  struct partition *p = &hash_table->partitions[s];
  struct task *self = &p->root_task;

  assert(batch_size <= NTASKS);

  // add unblock task to ready list
  TAILQ_INSERT_TAIL(&p->ready_list, &p->unblock_task, next);
  
  for (i = 0; i < batch_size; i++) {
      r = task_create(p);
      assert(r == 1);
    }

    for (i = 0; i < batch_size; i++) {
      int t = task_join(self);
    }

  lightweight_swapcontext(&self->ctx, &p->main_ctx);
}

void init_task(struct task *t, int tid, void (*fn)(), ucontext_t *next_ctx,
  int s)
{
  t->tid = tid;
  getcontext(&t->ctx);
  t->ctx.uc_stack.ss_sp = malloc(TASK_STACK_SIZE);
  t->ctx.uc_stack.ss_size = TASK_STACK_SIZE;
  t->ctx.uc_link = next_ctx;
  t->s = s;
  t->query = memalign(CACHELINE, sizeof(struct hash_query));
  makecontext(&t->ctx, fn, 2, s, t);
}

void reset_task(struct task *t)
{
  void *sp = t->ctx.uc_stack.ss_sp;
  ucontext_t *nc = t->ctx.uc_link;

  makecontext(&t->ctx, (void (*)())child_fn, 2, t->s, t->tid);

  assert(t->ctx.uc_stack.ss_sp == sp);
  assert(t->ctx.uc_link == nc);
}

void task_libinit(int s)
{
  int i;
  struct partition *p = &hash_table->partitions[s];

  TAILQ_INIT(&p->ready_list);
  TAILQ_INIT(&p->free_list);
  TAILQ_INIT(&p->wait_list);

  init_task(&p->root_task, ROOT_TASK_ID, root_fn, NULL, s);
  init_task(&p->unblock_task, UNBLOCK_TASK_ID, unblock_fn, NULL, s);

  for (i = 0; i < NTASKS; i++) {
    struct task *t = malloc(sizeof(struct task));
    assert(t);

    init_task(t, FIRST_TASK_ID + i, child_fn, &p->root_task.ctx, s);

    TAILQ_INSERT_TAIL(&p->free_list, t, next);
  }

  lightweight_swapcontext(&p->main_ctx, &p->root_task.ctx);
}

int task_create(struct partition *p)
{
  // if no more free threads return
  if (TAILQ_EMPTY(&p->free_list))
    return 0;

  // remove from free list and add to ready list
  struct task *t = TAILQ_FIRST(&p->free_list);
  TAILQ_REMOVE(&p->free_list, t, next);
  TAILQ_INSERT_TAIL(&p->ready_list, t, next);

  dprint("srv(%d): created task %d\n", t->s, t->tid);

  return 1;
}

void task_yield(struct partition *p, task_state state)
{
  struct task *self = p->current_task;

  dprint("srv(%d): task %d yielding state %d\n", self->s, self->tid, 
      state);

  if (state == TASK_STATE_WAITING) {
    TAILQ_INSERT_TAIL(&p->wait_list, self, next);
  } else {
    TAILQ_INSERT_TAIL(&p->ready_list, self, next);
  }

  self->state = state;

  lightweight_swapcontext(&self->ctx, &p->root_task.ctx);
}

int task_join(struct task *root_task)
{
  struct task *t;
  int s = root_task->s;
  struct partition *p = &hash_table->partitions[s];

schedule:
  t = TAILQ_FIRST(&p->ready_list);
  if (!t)
    dprint("srv(%d): EMPTY READYLIST??%d\n", s);

  assert(t);

  TAILQ_REMOVE(&p->ready_list, t, next); 
  p->current_task = t;

  dprint("srv(%d): root scheduling task %d\n", s, t->tid);

  lightweight_swapcontext(&root_task->ctx, &t->ctx);

  if (p->current_task->state == TASK_STATE_READY || 
    p->current_task->state == TASK_STATE_WAITING) {
    goto schedule;
  } else
    assert(p->current_task->state == TASK_STATE_FINISH);

  TAILQ_INSERT_HEAD(&p->free_list, p->current_task, next);
  
  return p->current_task->tid;
}

void task_destroy(struct task *t, struct partition *p)
{
  t->state = TASK_STATE_FINISH;

  lightweight_swapcontext(&t->ctx, &p->root_task.ctx);
}

void task_unblock(struct task *t)
{
  struct partition *p = &hash_table->partitions[t->s];

  assert(t->state == TASK_STATE_WAITING);
  
  TAILQ_REMOVE(&p->wait_list, t, next); 

  t->state = TASK_STATE_READY;

  TAILQ_INSERT_TAIL(&p->ready_list, t, next);
}

void lightweight_swapcontext(ucontext_t *out, ucontext_t *in)
{

__asm__(
#if defined(__x86_64__)
    // Good
#else
#error "Invalid architecture"
#endif

  /* Save the preserved registers, the registers used for passing args,
     and the return address.  */
  "popq %rbp\n"
  "movq  %rbx, "oRBX"(%rdi)\n"
  "movq  %rbp, "oRBP"(%rdi)\n"
  "movq  %r12, "oR12"(%rdi)\n"
  "movq  %r13, "oR13"(%rdi)\n"
  "movq  %r14, "oR14"(%rdi)\n"
  "movq  %r15, "oR15"(%rdi)\n"

  "movq  %rdi, "oRDI"(%rdi)\n"
  "movq  %rsi, "oRSI"(%rdi)\n"
  "movq  %rdx, "oRDX"(%rdi)\n"
  "movq  %rcx, "oRCX"(%rdi)\n"
  "movq  %r8, "oR8"(%rdi)\n"
  "movq  %r9, "oR9"(%rdi)\n"

  "movq  (%rsp), %rcx\n"
  "movq  %rcx, "oRIP"(%rdi)\n"
  "leaq  8(%rsp), %rcx\n"   /* Exclude the return address.  */
  "movq  %rcx, "oRSP"(%rdi)\n"

#if 0
  /* We have separate floating-point register content memory on the
     stack.  We use the __fpregs_mem block in the context.  Set the
     links up correctly.  */
  "leaq  "oFPREGSMEM"(%rdi), %rcx"
  "movq  %rcx, "oFPREGS"(%rdi)"

  /* Save the floating-point environment.  */
  "fnstenv (%rcx)"
  "stmxcsr "oMXCSR"(%rdi)"

  /* Restore the floating-point context.  Not the registers, only the
     rest.  */
  "movq  "oFPREGS"(%rsi), %rcx"
  "fldenv  (%rcx)"
  "ldmxcsr" oMXCSR"(%rsi)"
#endif

  /* Load the new stack pointer and the preserved registers.  */
  "movq  "oRSP"(%rsi), %rsp\n"
  "movq  "oRBX"(%rsi), %rbx\n"
  "movq  "oRBP"(%rsi), %rbp\n"
  "movq  "oR12"(%rsi), %r12\n"
  "movq  "oR13"(%rsi), %r13\n"
  "movq  "oR14"(%rsi), %r14\n"
  "movq  "oR15"(%rsi), %r15\n"

  /* The following ret should return to the address set with
  getcontext.  Therefore push the address on the stack.  */
  "movq  "oRIP"(%rsi), %rcx\n"
  "pushq %rcx\n"

  /* Setup registers used for passing args.  */
  "movq  "oRDI"(%rsi), %rdi\n"
  "movq  "oRDX"(%rsi), %rdx\n"
  "movq  "oRCX"(%rsi), %rcx\n"
  "movq  "oR8"(%rsi), %r8\n"
  "movq  "oR9"(%rsi), %r9\n"

  /* Setup finally  %rsi.  */
  "movq  "oRSI"(%rsi), %rsi\n"

  /* Clear rax to indicate success.  */
  "xorl  %eax, %eax\n"

  "ret\n"
);
}
