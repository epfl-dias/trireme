#include "headers.h"
#include "smphashtable.h"
#include "onewaybuffer.h"
#include "benchmark.h"
#include <sys/mman.h>

extern int batch_size;
extern int niters;
extern struct hash_table *hash_table;
extern struct benchmark *g_benchmark;
extern int dist_threshold;

int task_create(struct partition *p);
int task_join(struct task *root_task);
void task_destroy(struct task *t, struct partition *p);
void task_unblock(struct task *t);

#if NO_CUSTOM_SWAP_CONTEXT
#define lightweight_swapcontext(out, in) swapcontext(out, in)
#else
__attribute__ ((noinline))
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
//  "movq  %rdx, "oRDX"(%rdi)\n"
//  "movq  %rcx, "oRCX"(%rdi)\n"
//  "movq  %r8, "oR8"(%rdi)\n"
//  "movq  %r9, "oR9"(%rdi)\n"

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
//  "movq  "oRDX"(%rsi), %rdx\n"
//  "movq  "oRCX"(%rsi), %rcx\n"
//  "movq  "oR8"(%rsi), %r8\n"
//  "movq  "oR9"(%rsi), %r9\n"

  /* Setup finally  %rsi.  */
  "movq  "oRSI"(%rsi), %rsi\n"

  /* Clear rax to indicate success.  */
  "xorl  %eax, %eax\n"

  "ret\n"
);
}
#endif

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
          assert(t->nresponses < MAX_OPS_PER_QUERY);
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

struct hash_query *get_next_query(struct hash_table *hash_table, int s, 
    struct task *ctask)
{
  short idx = ctask->qidx;
  struct partition *p = &hash_table->partitions[s];
  struct hash_query *target_query = NULL;

  assert(idx < NQUERIES_PER_TASK);

  /* if one person is done, everybody is done */
  for (int i = 0; i < hash_table->nservers; i++) {
    if (hash_table->partitions[i].q_idx == niters)
        return NULL;
  }

  do {
    struct hash_query *next_query = &ctask->queries[idx];

    switch(next_query->state) {
      case HASH_QUERY_EMPTY:
      case HASH_QUERY_COMMITTED:
        if (p->q_idx < niters) {
          // we have txn left to run. get a fresh one and issue it
          g_benchmark->get_next_query(hash_table, s, next_query);
          target_query = next_query;
        } else {
          // we are done with all txns. only remaining aborted ones left.
          assert(p->q_idx == niters);
        }

        break;        
      case HASH_QUERY_ABORTED:
        /* this query aborted previously and we must have cycled back
         * issue this
         */
        target_query = next_query;
        break;

      default:
        assert(0);
    }

    idx = (++idx) % NQUERIES_PER_TASK;

  } while (idx != ctask->qidx && target_query == NULL);

  ctask->qidx = idx;

  return target_query;
}

void child_fn(int s, int tid)
{
  int r, ntxn;
  struct partition *p = &hash_table->partitions[s];
  struct task *self = p->current_task;
  assert(self && self->s == s);

  struct hash_query *next_query = get_next_query(hash_table, s, self);

  while (next_query) {

    dprint("srv(%d): task %d issuing txn \n", s, self->tid);

#if ENABLE_OP_BATCHING
    r = run_batch_txn(hash_table, self->s, next_query, self, r);
#else
    r = g_benchmark->run_txn(hash_table, self->s, next_query, self, r);
#endif

    if (r == TXN_ABORT) {
      next_query->state = HASH_QUERY_ABORTED;
      dprint("srv(%d):task %d txn aborted\n", s, self->tid);
      task_yield(p, TASK_STATE_READY);

      /* back off by sleeping. This is similar to the penalty in dbx100 */
      uint64_t penalty = URand(&p->seed, 0, ABORT_PENALTY);
      usleep(penalty / 1000);

      dprint("srv(%d): task %d rerunning aborted txn\n", s, self->tid);
    } else {
      next_query->state = HASH_QUERY_COMMITTED;
      p->ncommits++;
    }

    // After each txn, call process request
    if (dist_threshold != 100)
      process_requests(hash_table, s);

#if PRINT_PROGRESS
    if (p->q_idx % 100000 == 0) {
      printf("srv(%d): task %d finished %s(%" PRId64 " of %d)"
          " on key %" PRId64 "\n", 
          s, 
          self->tid, 
          next_query->ops[0].optype == OPTYPE_LOOKUP ? "lookup":"update", 
          p->q_idx, niters,
          next_query->ops[0].key);
    }
#endif

    next_query = get_next_query(hash_table, s, self);
  }

  //free(query);

  task_destroy(self, p);
}

void root_fn(int s, int tid)
{
  int i, r;
  struct partition *p = &hash_table->partitions[s];
  struct task *self = &p->root_task;

  assert(batch_size <= NTASKS - 2);

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
  //t->ctx.uc_stack.ss_sp = malloc(TASK_STACK_SIZE);
  t->ctx.uc_stack.ss_sp = mmap(NULL, TASK_STACK_SIZE, PROT_READ | PROT_WRITE,
      MAP_PRIVATE | MAP_ANONYMOUS | MAP_GROWSDOWN | MAP_STACK, -1, 0);
  
  t->ctx.uc_stack.ss_size = TASK_STACK_SIZE;
  t->ctx.uc_link = next_ctx;
  t->s = s;
  for (int i = 0; i < NQUERIES_PER_TASK; i++) {
    t->queries[i].state = HASH_QUERY_EMPTY;
    t->qidx = 0;
  }

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

  // tid starts from 2. HASHOP_TID_MASK permits 15. 
  assert(NTASKS <= 16);

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


