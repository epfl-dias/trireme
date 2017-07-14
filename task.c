#include "headers.h"
#include "smphashtable.h"
#include "onewaybuffer.h"
#include "benchmark.h"
#include <sys/mman.h>
#if ENABLE_DL_DETECT_CC
#include "mp_dl_detect_graph.h"
#include "glo.h"
#include "const.h"
#endif

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

//    dprint("srv(%d): Unblock task looping again\n", s);

    // we're done if waiters and ready list is empty
    if (p->q_idx == g_niters && TAILQ_EMPTY(&p->wait_list) && 
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
#if !defined(MIGRATION)
    for (int i = 0; i < g_nservers; i++) {
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
#endif
  }
}

#if ENABLE_DL_DETECT_CC
void dl_detect_fn(int s)
{
  struct partition *p = &hash_table->partitions[s];
  struct task *self = &p->unblock_task;
  struct box_array *boxes = hash_table->boxes;
  struct task *t;

  int detect_cycle = 0;
  struct dl_detect_graph_node src, rmv, deadlock_node;
  src.neighbors = (struct waiter_node *) calloc(g_nservers * g_batch_size, sizeof(struct waiter_node));
  rmv.neighbors = (struct waiter_node *) calloc(g_nservers * g_batch_size, sizeof(struct waiter_node));
  deadlock_node.neighbors = (struct waiter_node *) calloc(g_nservers * g_batch_size, sizeof(struct waiter_node));

  mp_dl_detect_init_dependency_graph();

  while (1) {

	if (hash_table->quitting && nready == 1)
		break;

    // read the adjacency lists from the other servers
    for (int i = 0; i < (g_nservers - 1); i ++) {
    	struct onewaybuffer *b = &boxes[s].boxes[i].out;
    	int count = b->wr_index - b->rd_index;
    	if (count) {
    		uint64_t data[ONEWAY_BUFFER_SIZE];
    		count = buffer_read_all(b, ONEWAY_BUFFER_SIZE, data, 0);
    		assert(count);
    		dprint("DL_DETECT SRV(%d): got %d messages from %d\n", s, count, i);

			int j = 0;
    		while (j < count) {
    			uint64_t op = data[j] & HASHOP_MASK;

    			switch(op) {
    			case DL_DETECT_ADD_DEP_SRC:
    				if (detect_cycle) {
						int added = mp_dl_detect_add_dependency(&src);
						if (added) {
							int deadlock = mp_dl_detect_detect_cycle(&src, &deadlock_node);
							if (deadlock) {
								uint64_t msg[2];
								msg[0] = MAKE_HASH_MSG(deadlock_node.fib, deadlock_node.srv, (unsigned long) deadlock_node.e, DL_DETECT_ABT_TXN);
								msg[1] = MAKE_TS_MSG(deadlock_node.opid, deadlock_node.ts);
								buffer_write_all(&boxes[g_nservers - 1].boxes[deadlock_node.sender_srv].in, 2, msg, 1);
								dprint("DL_DETECT SRV(%d): Sent ABORT message to srv %d for srv %d fib %d key %ld opid %d\n",
										s, deadlock_node.sender_srv, deadlock_node.srv, deadlock_node.fib, deadlock_node.e->key, deadlock_node.opid);
								mp_dl_detect_clear_dependencies(&src);
							}
						}
					}
					detect_cycle = 0;
    				src.sender_srv = i;
    				src.srv = HASHOP_GET_OPID(data[j]);
    				src.fib = HASHOP_GET_TID(data[j]);
    				src.e = (struct elem *) HASHOP_GET_VAL(data[j]);
    				src.ts = HASHOP_TSMSG_GET_TS(data[j + 1]);
    				src.opid = HASHOP_TSMSG_GET_OPID(data[j + 1]);
    				src.visited = 0;
    				src.waiters_size = 0;
					j += 2;
    				break;
    			case DL_DETECT_ADD_DEP_TRG:
    				src.neighbors[src.waiters_size].fib = HASHOP_GET_TID(data[j]);
    				src.neighbors[src.waiters_size].srv = HASHOP_GET_OPID(data[j]);
    				src.neighbors[src.waiters_size].ts = HASHOP_TSMSG_GET_TS(data[j + 1]);
    				src.neighbors[src.waiters_size].opid = HASHOP_TSMSG_GET_OPID(data[j + 1]);

    				dprint("DL_DETECT SRV(%d): received dependency (%d,%d,%ld) --> (%d,%d,%ld) for key %"PRIu64" from srv %d with opid %d\n",
									s, src.srv, src.fib, src.ts,
									src.neighbors[src.waiters_size].srv, src.neighbors[src.waiters_size].fib, src.neighbors[src.waiters_size].ts,
									src.e->key, i, src.opid);
    				src.waiters_size++;
					detect_cycle = 1;
					j += 2;
    				break;
    			case DL_DETECT_RMV_DEP_SRC:
    				rmv.sender_srv = i;
    				rmv.srv = HASHOP_GET_OPID(data[j]);
    				rmv.fib = HASHOP_GET_TID(data[j]);
    				rmv.ts = HASHOP_TSMSG_GET_TS(data[j + 1]);
    				rmv.opid = HASHOP_TSMSG_GET_OPID(data[j + 1]);
    				rmv.visited = 0;
    				rmv.waiters_size = 0;
    				j+=2;
    				break;
    			case DL_DETECT_RMV_DEP_TRG:
    				rmv.neighbors[rmv.waiters_size].fib = HASHOP_GET_TID(data[j]);
    				rmv.neighbors[rmv.waiters_size].srv = HASHOP_GET_OPID(data[j]);
    				rmv.neighbors[rmv.waiters_size].ts = HASHOP_TSMSG_GET_TS(data[j + 1]);
    				rmv.neighbors[rmv.waiters_size].opid = HASHOP_TSMSG_GET_OPID(data[j + 1]);
    				dprint("DL_DETECT SRV(%d): removed dependency (%d,%d,%ld) --> (%d,%d,%ld) from srv %d with opid %d\n",
								s, rmv.srv, rmv.fib, rmv.ts,
								rmv.neighbors[rmv.waiters_size].srv, rmv.neighbors[rmv.waiters_size].fib, rmv.neighbors[rmv.waiters_size].ts,
								i, rmv.opid);
    				rmv.waiters_size++;
					mp_dl_detect_remove_dependency(&rmv);

    				j+=2;
    				break;
    			default:
    				dprint("Do now know how to handle this point: op = %ld\n", op);
    			}
    		}
    		if (detect_cycle) {
				int added = mp_dl_detect_add_dependency(&src);
				if (added) {
					int deadlock = mp_dl_detect_detect_cycle(&src, &deadlock_node);
					if (deadlock) {
						uint64_t msg[2];
						msg[0] = MAKE_HASH_MSG(deadlock_node.fib, deadlock_node.srv, (unsigned long) deadlock_node.e, DL_DETECT_ABT_TXN);
						msg[1] = MAKE_TS_MSG(deadlock_node.opid, deadlock_node.ts);
						buffer_write_all(&boxes[g_nservers - 1].boxes[deadlock_node.sender_srv].in, 2, msg, 1);
						dprint("DL_DETECT SRV(%d): Sent ABORT message to srv %d for srv %d fib %d key %ld opid %d\n",
										s, deadlock_node.sender_srv, deadlock_node.srv, deadlock_node.fib, deadlock_node.e->key, deadlock_node.opid);
						mp_dl_detect_clear_dependencies(&src);
					}
				}
			}
			detect_cycle = 0;
    	}
    }


  }
}
#endif // ENABLE_DL_DETECT_CC

struct hash_query *get_next_query(struct hash_table *hash_table, int s, 
    struct task *ctask)
{
  short idx = ctask->qidx;
  struct partition *p = &hash_table->partitions[s];
  struct hash_query *target_query = NULL;

  assert(idx < NQUERIES_PER_TASK);

#if 0
  /* if one person is done, everybody is done */
  for (int i = 0; i < g_nservers; i++) {
    if (hash_table->partitions[i].q_idx == g_niters)
        return NULL;
  }
#endif
  if (hash_table->quitting)
      return NULL;

  do {
    struct hash_query *next_query = &ctask->queries[idx];

    switch(next_query->state) {
      case HASH_QUERY_EMPTY:
      case HASH_QUERY_COMMITTED:
        if (p->q_idx < g_niters) {
          // we have txn left to run. get a fresh one and issue it
          g_benchmark->get_next_query(hash_table, s, next_query);
          target_query = next_query;
        } else {
          // we are done with all txns. only remaining aborted ones left.
          assert(p->q_idx == g_niters);
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
    r = run_batch_txn(hash_table, self->s, next_query, self);
#else
    r = g_benchmark->run_txn(hash_table, self->s, next_query, self);
#endif

#if MIGRATION
    s = self->s;
    p = &hash_table->partitions[s];
#endif
    
    if (r == TXN_ABORT) {
#if GATHER_STATS
      self->naborts++;
#endif
      next_query->state = HASH_QUERY_ABORTED;
      dprint("srv(%d):task %d txn aborted\n", s, self->tid);
      //task_yield(p, TASK_STATE_READY);



#if !defined(ENABLE_ABORT_BACKOFF)
#if defined(SHARED_EVERYTHING) && !defined(ENABLE_SILO_CC)
      /* back off by sleeping. This is similar to the penalty in dbx100. 
       * Backoff helps 2pl protocols a lot but not silo that much. So use only
       * with 2pl. So add delay only for SE protocols. Also, adding penalty 
       * made things much worse for messaging. So use only for SE
       */
//#define ENABLE_ABORT_BACKOFF 1
#endif
#endif

#if ENABLE_ABORT_BACKOFF
      uint64_t penalty = URand(&p->seed, 0, ABORT_PENALTY);
      usleep(penalty / 1000);
#endif

      dprint("srv(%d): task %d rerunning aborted txn\n", s, self->tid);
    } else {
#if GATHER_STATS
      self->ncommits++;  
#endif
      next_query->state = HASH_QUERY_COMMITTED;
    }

    // After each txn, call process request
#if !defined(SHARED_EVERYTHING) && !defined(SHARED_NOTHING)
    if (g_dist_threshold != 100)
      process_requests(hash_table, s);
#endif

#if PRINT_PROGRESS
    if (p->q_idx % 100000 == 0) {
      printf("srv(%d): task %d finished %s(%" PRId64 " of %d)"
          " on key %" PRId64 "\n", 
          s, 
          self->tid, 
          next_query->ops[0].optype == OPTYPE_LOOKUP ? "lookup":"update", 
          p->q_idx, g_niters,
          next_query->ops[0].key);
    }
#endif
    
#if MIGRATION
    if (self->s != self->origin) {
        self->target = self->origin;
        task_yield(p, TASK_STATE_MIGRATE);
        s = self->s;
        p = &hash_table->partitions[s];
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

  assert(g_batch_size <= NTASKS - 2);

  // add unblock task to ready list
  TAILQ_INSERT_TAIL(&p->ready_list, &p->unblock_task, next);
  
  for (i = 0; i < g_batch_size; i++) {
      r = task_create(p);
      assert(r == 1);
    }

    for (i = 0; i < g_batch_size; i++) {
      int t = task_join(self);
    }

  lightweight_swapcontext(&self->ctx, &p->main_ctx);
}

void init_task(struct task *t, int tid, void (*fn)(), ucontext_t *next_ctx,
  int s)
{
  t->tid = tid;
  t->g_tid = s * g_batch_size + tid - 2;
  getcontext(&t->ctx);
  //t->ctx.uc_stack.ss_sp = malloc(TASK_STACK_SIZE);
  t->ctx.uc_stack.ss_sp = mmap(NULL, TASK_STACK_SIZE, PROT_READ | PROT_WRITE,
      MAP_PRIVATE | MAP_ANONYMOUS | MAP_GROWSDOWN | MAP_STACK, -1, 0);
  
  t->ctx.uc_stack.ss_size = TASK_STACK_SIZE;
  t->ctx.uc_link = next_ctx;
  t->s = s;
  t->origin = s;
#if defined(GATHER_STATS)
  t->run_time = 0;
  t->times_scheduled = 0;
  g_tasks[s][tid] = t;
  t->ncommits = 0;
  t->naborts = 0;
#endif
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
  } else if (state == TASK_STATE_MIGRATE) {
#if !defined(MIGRATION)
      assert(0);
#endif
  } else {
    TAILQ_INSERT_TAIL(&p->ready_list, self, next);
  }

  self->state = state;

  lightweight_swapcontext(&self->ctx, &p->root_task.ctx);
}

void send_migration(struct task *ctask, int target) {
    uint64_t msg;
    msg = MAKE_HASH_MSG(0, 0, (long unsigned int)ctask, HASHOP_MIGRATE);
    buffer_write_all(&hash_table->boxes[ctask->s].boxes[target].in, 1, &msg, 1);
}

void task_resume_migration(struct task *ctask, struct partition *p) {
    assert(ctask->state == TASK_STATE_MIGRATE);
    ctask->state = TASK_STATE_READY;
    TAILQ_INSERT_TAIL(&p->ready_list, ctask, next);
}

int task_join(struct task *root_task)
{
  struct task *t;
  int s = root_task->s;
  struct partition *p = &hash_table->partitions[s];

  double t_st = now();
  double t_en = t_st;
schedule:
#if defined(MIGRATION)
  if (hash_table->quitting) {
      goto scheduleend;
  }
    assert(root_task==&p->root_task);
#endif
  t = TAILQ_FIRST(&p->ready_list);
  t->s = s;

  if (!t)
    dprint("srv(%d): EMPTY READYLIST??%d\n", s);

  assert(t);

  TAILQ_REMOVE(&p->ready_list, t, next); 
  p->current_task = t;

  dprint("srv(%d): root scheduling task %d\n", s, t->tid);

#if GATHER_STATS
  t->times_scheduled++;
  t_en = now();
  root_task->run_time += t_en - t_st;
  t_st = t_en;
#endif
  lightweight_swapcontext(&root_task->ctx, &t->ctx);
#if GATHER_STATS
  t_en = now();
  t->run_time += t_en - t_st;
  t_st = t_en;
#endif

  if (p->current_task->state == TASK_STATE_READY || 
    p->current_task->state == TASK_STATE_WAITING) {
    goto schedule;
  } else if (p->current_task->state == TASK_STATE_MIGRATE) {
      send_migration(p->current_task, p->current_task->target);
      goto schedule;
  } else
    assert(p->current_task->state == TASK_STATE_FINISH);

scheduleend:
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


