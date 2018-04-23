#include <sys/sysinfo.h>

#include "headers.h"
#include "onewaybuffer.h"
#include "partition.h"
#include "smphashtable.h"
#include "benchmark.h"
#include "tpcc.h"
#include "plmalloc.h"
#include "twopl.h"
#include "mvto.h"
#include "mv2pl.h"
#include "svdreadlock.h"
#include "mvdreadlock.h"
#include "silo.h"
#include "se_dl_detect_graph.h"

const char *optype_str[] = {"","lookup","insert","update","release"};
#define OPTYPE_STR(optype) optype_str[optype >> 60]


// Forward declarations
void *hash_table_server(void* args);
int is_value_ready(struct elem *e);
void smp_hash_doall(struct task *ctask, struct hash_table *hash_table,
    int client_id, int server, int nqueries, struct hash_op **queries,
    void **values, int opid);
int smp_hash_lookup(struct task *ctask, struct hash_table *hash_table,
    int client_id, int server, hash_key key, short opid);
int smp_hash_update(struct task *ctask, struct hash_table *hash_table,
    int client_id, int server, hash_key key, short opid);


struct hash_table *create_hash_table()
{
#if ENABLE_ASYMMETRIC_MESSAGING
  int nrecs_per_partition = g_nrecs / g_nhot_servers;
#elif 0//(!defined(SHARED_EVERYTHING) && defined(ENABLE_DL_DETECT_CC))
  int nrecs_per_partition = g_nrecs / (g_nservers - 1);
#else
  int nrecs_per_partition = g_nrecs / g_nservers;
#endif
  struct hash_table *hash_table = (struct hash_table *)malloc(sizeof(struct hash_table));

  hash_table->keys = NULL;
  hash_table->partitions = memalign(CACHELINE, g_nservers * sizeof(struct partition));
  hash_table->g_partition = memalign(CACHELINE, sizeof(struct partition));
  hash_table->boxes = memalign(CACHELINE, MAX_CLIENTS * sizeof(struct box_array));

  pthread_mutex_init(&hash_table->create_client_lock, NULL);

  for (int i = 0; i < g_nservers; i++) {

#if SHARED_EVERYTHING
    init_hash_partition(&hash_table->partitions[i], g_nrecs,
        i == 0 /*alloc buckets only for first partition*/);

    /* make other partition buckets point to first partition's buckets */
    if (i > 0)
      hash_table->partitions[i].table = hash_table->partitions[0].table;

#else
    init_hash_partition(&hash_table->partitions[i], nrecs_per_partition, 1);
#endif
  }
#if 0//(!defined(SHARED_EVERYTHING) && defined(ENABLE_DL_DETECT_CC))
  init_hash_partition(hash_table->g_partition, g_nrecs / (g_nservers - 1), 1);
#else
  init_hash_partition(hash_table->g_partition, g_nrecs / g_nservers, 1);
#endif

  hash_table->threads = (pthread_t *)malloc(g_nservers * sizeof(pthread_t));
  hash_table->thread_data = (struct thread_args *)malloc(g_nservers * sizeof(struct thread_args));

  create_hash_table_client(hash_table);

  /*
  if (g_alpha != 0) {
    printf("Generating zipfian distribution: ");
    fflush(stdout);

    hash_table->keys = zipf_get_keys(g_alpha, g_nrecs, g_niters * g_nservers);
  } else
    hash_table->keys = NULL;
  */

  return hash_table;
}

void destroy_hash_table(struct hash_table *hash_table)
{
  int i;
  size_t act_psize, dbg_psize;
  act_psize = 0;

#if SHARED_EVERYTHING
  /* call destroy partition on first partition only for shared everything case
   * In shared nothing and trireme, partition destruction happens in each
   * thread.
   */
  for (i = 0; i < g_nservers; i++) {
    act_psize += hash_table->partitions[i].size;
  }

  dbg_psize = destroy_hash_partition(&hash_table->partitions[0]);
  //assert(act_psize == dbg_psize);

#endif

  /* XXX: What about the global partition? */
  //destroy_hash_partition(&hash_table->g_partition, atomic_release_value_);

  free(hash_table->partitions);
  free(hash_table->g_partition);

  for (int i = 0; i < g_nservers; i++)
    free(hash_table->boxes[i].boxes);

  free(hash_table->boxes);

  free(hash_table->threads);
  free(hash_table->thread_data);
  free(hash_table);
}

#if 0
void start_hash_table_servers(struct hash_table *hash_table)
{
#if ENABLE_DL_DETECT_CC
#include "glo.h"
//	DL_detect_init(&dl_detector);
	se_dl_detect_init_dependency_graph();
#if !defined(SHARED_EVERYTHING)
#include "twopl.h"
	dl_detect_init_data_structures();
#endif
#endif

  int r;
  void *value;
  nready = hash_table->quitting = 0;

  assert(NCORES >= g_active_servers);

  for (int i = 0; i < g_active_servers; i++) {
    hash_table->thread_data[i].id = i;
#if ENABLE_VIRTUALIZATION
    hash_table->thread_data[i].core = i;
#else
    hash_table->thread_data[i].core = coreids[i];
#endif
    hash_table->thread_data[i].hash_table = hash_table;

    printf("Assinging core %d to srv %d\n", hash_table->thread_data[i].core, i);

    r = pthread_create(&hash_table->threads[i], NULL, hash_table_server, (void *) (&hash_table->thread_data[i]));
    assert(r == 0);
  }

  /* wait for everybody to start */
  while (nready != g_active_servers) ;

  /* sleep for preconfigured time */
  usleep(RUN_TIME);

  hash_table->quitting = 1;

  for (int i = 0; i < g_active_servers; i++) {
    r = pthread_join(hash_table->threads[i], &value);
    assert(r == 0);
  }
}

void start_hash_table_servers_hotplug(struct hash_table *hash_table, int hotplugged_servers)
{
	int r;
	void *value;
	nready = 0;

	int old_g_active_servers = g_active_servers;
	g_active_servers += hotplugged_servers;
	assert(g_active_servers <= g_nservers);
	assert(g_active_servers <= NCORES);


	for (int i = old_g_active_servers; i < g_active_servers; i++) {
		hash_table->thread_data[i].id = i;
		hash_table->thread_data[i].core = i;
		hash_table->thread_data[i].hash_table = hash_table;
		printf("Assinging core %d to srv %d\n", hash_table->thread_data[i].core, i);

		r = pthread_create(&hash_table->threads[i], NULL, hash_table_server, (void *) (&hash_table->thread_data[i]));
		assert(r == 0);
	}

	while (nready != g_active_servers);

	usleep(RUN_TIME);


	for (int i = 0; i < g_active_servers; i++) {
		r = pthread_join(hash_table->threads[i], &value);
		assert(r == 0);
	}
}
#endif //0

void create_hash_table_client(struct hash_table *hash_table)
{
  for (int i = 0; i < g_nservers; i++) {
    hash_table->boxes[i].boxes = memalign(CACHELINE, g_nservers * sizeof(struct box));
    assert((unsigned long) &hash_table->boxes[i] % CACHELINE == 0);

    for (int j = 0; j < g_nservers; j++) {
      memset((void*)&hash_table->boxes[i].boxes[j], 0, sizeof(struct box));
      assert((unsigned long) &hash_table->boxes[i].boxes[j].in % CACHELINE == 0);
      assert((unsigned long) &hash_table->boxes[i].boxes[j].out % CACHELINE == 0);
    }
  }
}

struct elem *local_txn_op(struct task *ctask, int s, struct txn_ctx *ctx,
    struct partition *p, struct hash_op *op)
{
  struct elem *e;
  uint32_t t = op->optype;
  struct lock_entry *l = NULL;
#if ENABLE_DL_DETECT_CC
  int notification = 0;
#endif

  int r;

#if GATHER_STATS
  if (t == OPTYPE_LOOKUP)
    p->nlookups_local++;
  else
    p->nupdates_local++;
#endif

  switch (t) {
    case OPTYPE_INSERT:
      // should not get a insert to item partition
      assert (p != hash_table->g_partition);

      e = hash_insert(p, op->key, op->size, NULL);
      assert(e);

      // set ref count to 1 here so that cc algo will set it appropriate below
      e->ref_count = 1;
      p->ninserts++;

#if SHARED_EVERYTHING
#if defined(ENABLE_MVTO) || defined(ENABLE_MV2PL)
      // XXX: ADD SUPPORT FOR INSERTIONS
      assert(0);

#else
      if (!selock_acquire(p, e, t, ctx->ts)) {
        return NULL;
      }
#endif //ENABLE_MVTO

#elif SHARED_NOTHING
      // no logical locks. do nothing
#else
#if ENABLE_WAIT_DIE_CC
      r = wait_die_acquire(s, p, s /* client id */, ctask->tid, 0 /* opid */,
          e, t, ctx->ts, &l);
#elif ENABLE_BWAIT_CC
      r = bwait_acquire(s, p, s /* client id */, ctask->tid, 0 /* opid */,
          e, t, &l);
#elif ENABLE_NOWAIT_CC
      r = no_wait_acquire(e, t);
#elif ENABLE_SILO_CC
      r = LOCK_SUCCESS;
#elif ENABLE_DL_DETECT_CC
      r = dl_detect_acquire(s, p, s /* client id */, ctask->tid, ctx->nops /* opid */,
      		  e, t, &l, ctx->ts, &notification);
#elif ENABLE_MV2PL
      // XXX: Inserts not yet supported
      assert(0);

#elif !defined(SHARED_EVERYTHING) && !defined(SHARED_NOTHING)
#error "No CC algorithm specified"
#endif //IF_ENABLE_WAIT_DIE_CC

      // insert can only succeed
      assert (r == LOCK_SUCCESS);

#endif //IF_SHARED_EVERYTHING

      break;

    case OPTYPE_LOOKUP:
    case OPTYPE_UPDATE:
      e = hash_lookup(p, op->key);


      //XXX I had to change this part for implementing the cursor
      //printf("srv(%d): lookup key %"PRIu64"\n", s, op->key);
      if (e == NULL) {
        //assert(0);
        return NULL;
      }
      // if this is the ITEM TID, we are done
      if (p == hash_table->g_partition)
        break;

#if SHARED_EVERYTHING

#if ENABLE_MVTO
      if (!(e = mvto_acquire(p, e, t, ctx->ts)))
          return NULL;
#elif defined(ENABLE_MV2PL) || defined(ENABLE_MV2PL_DRWLOCK)
      if (!(e = mv2pl_acquire(p, e, t)))
          return NULL;
#elif defined(ENABLE_SVDREADLOCK_CC) || defined(ENABLE_FSVDREADLOCK_CC)
      if (!(e = svdreadlock_acquire(p, e, t)))
          return NULL;
#elif ENABLE_MVDREADLOCK_CC
      if (!(e = mvdreadlock_acquire(p, e, t)))
          return NULL;
#else
      if (!selock_acquire(p, e, t, ctx->ts)) {
          //This is where value is set to 0
          return NULL;
      }
#endif //ENABLE_MVTO

#elif SHARED_NOTHING
      /* Do absolutely nothing for shared nothing as it proceeds by first
       * getting partition locks. So there is no need to get record locks
       * as access to the whole partition itself is serialized
       */
#else

#if ENABLE_WAIT_DIE_CC
      r = wait_die_acquire(s, p, s /* client id */, ctask->tid, 0 /* opid */,
          e, t, ctx->ts, &l);
#elif ENABLE_BWAIT_CC
      r = bwait_acquire(s, p, s /* client id */, ctask->tid, 0 /* opid */,
          e, t, &l);
#elif ENABLE_NOWAIT_CC
      r = no_wait_acquire(e, t);
#elif ENABLE_SILO_CC
      r = LOCK_SUCCESS;
#elif ENABLE_DL_DETECT_CC
      r = dl_detect_acquire(s, p, s /* client id */, ctask->tid, ctx->nops /* opid */,
                e, t, &l, ctx->ts, &notification);
#elif ENABLE_MV2PL
      r = del_mv2pl_acquire(e, t);
#elif !defined(SHARED_EVERYTHING) && !defined(SHARED_NOTHING)
#error "No CC algorithm specified"
#endif //IF_ENABLE_WAIT_DIE

      if (r == LOCK_SUCCESS) {
        ; // great we have the lock
      } else if (r == LOCK_WAIT) {

        /* we have to spin now until value is ready. But we also need to
         * service other requests
         */
        assert(l);

#if (!defined(SHARED_EVERYTHING) && defined(ENABLE_DL_DETECT_CC))
		while ((!l->ready) && (!notification)){
      task_yield(p, TASK_STATE_READY);
    }


		if (l->ready == LOCK_ABORT_NXT) {
			dl_detect_release(s, p, s, ctask->tid, ctx->nops, e, 0);
			r = LOCK_ABORT;
			return NULL;
		} else {
        	assert((l->ready == 1) || (notification == 1));
        }

#else
        while (!l->ready)
            task_yield(p, TASK_STATE_READY);

        assert(l->ready == 1);
#endif

      } else {
        // busted
        assert(r == LOCK_ABORT);

#if defined(ENABLE_BWAIT_CC)
        assert(0);
#elif (!defined(SHARED_EVERYTHING) && defined(ENABLE_DL_DETECT_CC))
//        dl_detect_release(s, p, s, ctask->tid, ctx->nops, e, 0);
#endif

        return NULL;
      }
#endif //IF_SHARED_EVERYTHING

      break;

    case OPTYPE_DELETE:
      e = hash_lookup(p, op->key);
      if (!e) {
        return NULL;
      }
      hash_remove(p, e);
      break;
      //XXX I have not add any support for share_everything and share_nothing

    default:
      assert(0);
      break;
  }
  return e;
}

void *txn_op(struct task *ctask, struct hash_table *hash_table, int s,
    struct hash_op *op, int target)
{
  void *value = NULL;
  struct elem *e = NULL;
  struct txn_ctx *ctx = &ctask->txn_ctx;
  struct partition *p = &hash_table->partitions[s];
  struct partition *l_p = NULL;

#if MIGRATION && SHARED_EVERYTHING
  int is_local = (s == target);
  l_p = p;
#elif SHARED_EVERYTHING
  int is_local = 1;
  l_p = p;
#elif SHARED_NOTHING
  int is_local = 1;
  l_p = &hash_table->partitions[target];
#else
  int is_local = (s == target);
  l_p = p;
#endif

  //ugly hack to make item table global in all cases
  if ((op->key & TID_MASK) == ITEM_TID) {
    l_p = hash_table->g_partition;
    is_local = 1;
  }

  dprint("srv(%d): issue %s %s op key %" PRIu64 "\n",
    s, is_local ? "local":"remote",
    op->optype == OPTYPE_LOOKUP ? "lookup":"update", op->key);

#if defined(MIGRATION)
  if (!is_local) {
      ctask->target = target;
      task_yield(p, TASK_STATE_MIGRATE);
      s = ctask->target;
      p = &hash_table->partitions[s];
      l_p = p;
      is_local = 1;
  }
#endif

  // if this is us, just call local procedure

  if (is_local) {

    //assert(op->key >= s * l_p->nrecs && op->key < (s * l_p->nrecs + l_p->nrecs));

    assert (l_p);

    e = local_txn_op(ctask, s, ctx, l_p, op);
#if ENABLE_SILO_CC
    // we never lookup a non-existent record. so this should always succeed
    assert(e);
    value = e->value;
#else
    // it is possible for a local txn to fail as someone else might have
    // acquired a lock before us
    if (e){
      value = e->value;
    }
    else{
      value = NULL;
    }

#endif

  } else {
#if defined(SHARED_EVERYTHING) || defined(SHARED_NOTHING)
    assert(0);
#endif

#if ENABLE_SILO_CC
    // in silo's case, there is no locking in any of the operations. so we need
    // to proceed as though operation is local to avoid unnecessary overhead.
    // At validation time, we will do the locking. so we use messaging there.

#if !defined(ENABLE_INDEX_LATCH)

    // Assumes with tpcc that index latching is enabled
    assert(g_benchmark == &micro_bench);

#endif// EN_INDEX_LATCH

    l_p = &hash_table->partitions[target];
    e = local_txn_op(ctask, target, ctx, l_p, op);

    // can never fail as we don't lookup non-existent records or lock them
    assert(e);
    value = (void *) e;

#else

    // Some other CC protocol (not Silo). This is a remote access
    // call the corresponding authority and block
    smp_hash_doall(ctask, hash_table, s, target, 1, &op, &value, ctx->nops);

#endif //EN_SILO_CC

    // the remote server can fail to return us data if someone else has a lock
    if (value) {
      e = (struct elem *)value;
      value = e->value;
      assert(value);
    }
  }

#if SHARED_NOTHING
  /* in shared nothing case, aborts are impossible. So don't bother
   * making copies of data as we wont need it
   */
#else
  if (value) {
    struct op_ctx *octx = &ctx->op_ctx[ctx->nops];
    octx->optype = op->optype;
    octx->e = e;
#if SHARED_EVERYTHING
    octx->target = s;
#else
    octx->target = target;
#endif //SE

#if ENABLE_SILO_CC

    /* in silo case, we make copy in lookup/update cases for later
     * validation. While we make a copy, someone could be writing. To avoid
     * this, we need to latch before making copy
     */
    if (op->optype == OPTYPE_LOOKUP || op->optype == OPTYPE_UPDATE) {
        octx->data_copy = plmalloc_ealloc(p);
        octx->data_copy->value = plmalloc_alloc(p, e->size);

#if SILO_USE_ATOMICS
        uint64_t old_tid = 0, new_tid = 1;
        while (old_tid != new_tid) {
            old_tid = e->tid;

            while (old_tid & SILO_LOCK_BIT) {
#if SHARED_EVERYTHING
                _mm_pause();
#else
                task_yield(p, TASK_STATE_READY);
#endif
                old_tid = e->tid;
            }

            memcpy(octx->data_copy->value, e->value, e->size);

            COMPILER_BARRIER();

            new_tid = e->tid;
        }

        assert(!(old_tid & SILO_LOCK_BIT));
        octx->tid_copy = old_tid;
#else
        silo_latch_acquire(s, e);
        octx->tid_copy = e->tid;
        memcpy(octx->data_copy->value, e->value, e->size);
        silo_latch_release(s, e);
#endif //IF_SILO_USE_ATOMICS

        dprint("srv(%d): adding %s %s op key %" PRIu64 " ctx-nops %d"
                " to rd/wt set, etid %d, copytid %d\n",
                s, is_local ? "local":"remote",
                op->optype == OPTYPE_LOOKUP ? "lookup":"update", op->key,
                ctx->nops, octx->e->tid, octx->tid_copy);

        // pass back the newly created value to keep read/write sets thread local
        value = octx->data_copy->value;
    } else {
        octx->data_copy = NULL;
    }

#else

    // in 2pl, we need to make copy only for updates. we use the same for mvcc
    if (op->optype == OPTYPE_UPDATE) {
        octx->data_copy = plmalloc_ealloc(p);
        octx->data_copy->value = plmalloc_alloc(p, e->size);
        memcpy(octx->data_copy->value, e->value, e->size);

#if defined(ENABLE_MV2PL) || defined(ENABLE_MV2PL_DRWLOCK) || defined(ENABLE_SVDREADLOCK_CC) || defined(ENABLE_MVDREADLOCK_CC) || defined(ENABLE_FSVDREADLOCK_CC)
        value = octx->data_copy->value;
#endif
    } else {
        octx->data_copy = NULL;
    }

#endif // EN_SILO_CC

    ctx->nops++;
  }
#endif //SN

  dprint("srv(%d): done %s %s op key %" PRIu64 " ctx-nops %d status %s\n",
          s, is_local ? "local":"remote",
          op->optype == OPTYPE_LOOKUP ? "lookup":"update", op->key, ctx->nops,
          value ? "ok" : "fail");
  return value;
}

void txn_start(struct hash_table *hash_table, int s,
    struct txn_ctx *ctx)
{
  ctx->nops = 0;
  ctx->ts = read_tsc();
#if GATHER_STATS
  ctx->start_time = now();
#endif
}

int txn_finish(struct task *ctask, struct hash_table *hash_table, int s,
    int status, int mode, short *opids)
{
  struct txn_ctx *ctx = &ctask->txn_ctx;
  struct partition *p = &hash_table->partitions[s];
  int nops = ctx->nops;
  int nrels;

#if GATHER_STATS
  if (status == TXN_COMMIT) {
      double end_time = now();
      double txn_latency = end_time - ctx->start_time;
      if (txn_latency > p->max_txn_latency)
          p->max_txn_latency = txn_latency;

      if (txn_latency < p->min_txn_latency)
          p->min_txn_latency = txn_latency;

      p->total_txn_latency += txn_latency;
  }
#endif

#if ENABLE_SILO_CC
  // at this point, we need to validate the txn under silo to determine if it
  // can complete or not
  assert(status == TXN_COMMIT);

  status = silo_validate(ctask, hash_table, s);
#elif defined(ENABLE_MV2PL) || defined(ENABLE_MV2PL_DRWLOCK)

  if (status == TXN_COMMIT)
      status = mv2pl_validate(ctask, hash_table, s);
  else
      mv2pl_abort(ctask, hash_table, s);
#elif defined(ENABLE_SVDREADLOCK_CC) || defined(ENABLE_FSVDREADLOCK_CC)
  if (status == TXN_COMMIT)
      status = svdreadlock_validate(ctask, hash_table, s);
  else
      svdreadlock_abort(ctask, hash_table, s);
#elif defined(ENABLE_MVDREADLOCK_CC)

  if (status == TXN_COMMIT)
      status = mvdreadlock_validate(ctask, hash_table, s);
  else
      mvdreadlock_abort(ctask, hash_table, s);

#elif (!defined(SHARED_EVERYTHING) && defined(ENABLE_DL_DETECT_CC))
    int release_notify = 1;
//    if (status != TXN_ABORT) {
//  	  uint64_t msg[2];
//  	  msg[0] = MAKE_HASH_MSG(ctask->tid - 2, ctask->s, (unsigned long) s, DL_DETECT_CLR_DEP);
//  	  msg[1] = ctx->ts;
//  	  dprint("srv (%d): clearing dependencies for srv %d\n", s, ctask->s);
//  	  struct box_array *boxes = hash_table->boxes;
//  	  buffer_write_all(&boxes[g_nservers - 1].boxes[s].out, 2, msg, 1);
//    }
#endif

  while (--nops >= 0) {
    struct op_ctx *octx = &ctx->op_ctx[nops];
    int t = octx->optype;

    if (!octx->e) {
      // only possible in batch mode or if benchmark is tpcc
      assert(mode == TXN_BATCH || g_benchmark == &tpcc_bench);
      continue;
    }

    // lookups on item table do not need anything
    if ((octx->e->key & TID_MASK) == ITEM_TID) {
      assert(octx->optype == OPTYPE_LOOKUP);
      continue;
    }

#if MIGRATION
    if (octx->target != s) {
        ctask->target = octx->target;
        task_yield(p, TASK_STATE_MIGRATE);
        s = ctask->s;
        p = &hash_table->partitions[s];
        assert(s == get_affinity());
    }
#endif

    switch (t) {
      case OPTYPE_LOOKUP:

#if ENABLE_SILO_CC
            // in Silo, we always have local data copy in readset. Free it.
            assert (octx->data_copy);
            plmalloc_free(p, octx->data_copy->value, octx->e->size);
            plmalloc_efree(p, octx->data_copy);
#else
            assert (octx->data_copy == NULL);
#endif //IF_SILO

        // release element
        if (octx->target == s) {

#if SHARED_EVERYTHING
          selock_release(p, octx);
#else
#if ENABLE_WAIT_DIE_CC
          wait_die_release(s, p, s, ctask->tid,
              opids ? opids[nops]: 0, octx->e);
#elif ENABLE_BWAIT_CC
          bwait_release(s, p, s, ctask->tid,
              opids ? opids[nops]: 0, octx->e);
#elif ENABLE_NOWAIT_CC
          no_wait_release(p, octx->e);
#elif defined(ENABLE_SILO_CC) || defined(ENABLE_MV2PL) || defined(ENABLE_SVDREADLOCK_CC) || defined(ENABLE_MVDREADLOCK_CC)|| defined(ENABLE_FSVDREADLOCK_CC) || defined(ENABLE_MV2PL_DRWLOCK)
          ; // do nothing. everything is done by validate function
#elif ENABLE_DL_DETECT_CC
          dl_detect_release(s, p, s, ctask->tid,
                        opids ? opids[nops] : 0, octx->e, release_notify);
#elif !defined(SHARED_EVERYTHING) && !defined(SHARED_NOTHING)
#error "No CC algorithm specified"
#endif //IF_ENABLE_WAIT_DIE_CC
#endif //IF_SHARED_EVERYTHING
        } else {
#if MIGRATION
            assert(0);
#endif

#if defined(ENABLE_SILO_CC) || defined(ENABLE_MV2PL) || defined(ENABLE_SVDREADLOCK_CC) || defined(ENABLE_MVDREADLOCK_CC)|| defined(ENABLE_FSVDREADLOCK_CC) || defined(ENABLE_MV2PL_DRWLOCK)
          ; // do nothing. everything is done by validate function
#else

          // not silo. send out release messages
            mp_mark_ready(hash_table, s, octx->target, ctask->tid,
                    opids ? opids[nops] : 0, octx->e, octx->optype);
#endif // EN_SILO_CC
        }

        break;

      case OPTYPE_UPDATE:
        // should never get updates to item table
        assert((octx->e->key & TID_MASK) != ITEM_TID);

        assert(octx->data_copy);

        size_t size = octx->e->size;

        // In silo's case, we don't need to copy back anything as read/write
        // sets were already thread local. so aborts are no ops
        // In 2pl case, the txn would have updated "live" record and not the
        // copy. So we need to abort by reverting the update.
        if (status == TXN_ABORT) {
#if defined(ENABLE_SILO_CC) || defined(ENABLE_MV2PL) || defined(ENABLE_SVDREADLOCK_CC)|| defined(ENABLE_MVDREADLOCK_CC)|| defined(ENABLE_FSVDREADLOCK_CC) || defined(ENABLE_MV2PL_DRWLOCK)
            ;
#else
            memcpy(octx->e->value, octx->data_copy->value, size);
#endif //IF_ENABLE_SILO
        }

#if ENABLE_MVTO
        // in case of mvcc, we free the data copy only on aborts.
        // otherwise, we version it
        if (status == TXN_ABORT) {
            plmalloc_free(p, octx->data_copy->value, size);
            plmalloc_efree(p, octx->data_copy);
        }
#else
        plmalloc_free(p, octx->data_copy->value, size);
        plmalloc_efree(p, octx->data_copy);
#endif

        if (octx->target == s) {
#if SHARED_EVERYTHING

#if ENABLE_MVTO
            if (status == TXN_COMMIT)
                mvto_release(p, octx->e, octx->data_copy);
            else
                mvto_release(p, octx->e, NULL);
#else
            selock_release(p, octx);
#endif //ENABLE_MVTO

#else
#if ENABLE_WAIT_DIE_CC
          wait_die_release(s, p, s, ctask->tid,
              opids ? opids[nops] : 0, octx->e);
#elif ENABLE_BWAIT_CC
          bwait_release(s, p, s, ctask->tid,
              opids ? opids[nops] : 0, octx->e);
#elif ENABLE_NOWAIT_CC
          no_wait_release(p, octx->e);
#elif defined(ENABLE_SILO_CC) || defined(ENABLE_MV2PL) || defined(ENABLE_SVDREADLOCK_CC) || defined(ENABLE_MVDREADLOCK_CC)|| defined(ENABLE_FSVDREADLOCK_CC) || defined(ENABLE_MV2PL_DRWLOCK)
          ; // do nothing. everything is done by validate function
#elif defined(ENABLE_DL_DETECT_CC)
          dl_detect_release(s, p, s, ctask->tid,
                        opids ? opids[nops] : 0, octx->e, release_notify);
#elif !defined(SHARED_EVERYTHING) && !defined(SHARED_NOTHING)
#error "No CC algorithm specified"
#endif //ENABLE_WAIT_DIE_CC
#endif //SHARED_EVERYTHING
        } else {
#if MIGRATION
            assert(0);
#endif

#if defined(ENABLE_SILO_CC) || defined(ENABLE_MV2PL) || defined(ENABLE_SVDREADLOCK_CC) || defined(ENABLE_MVDREADLOCK_CC) || defined(ENABLE_FSVDREADLOCK_CC) || defined(ENABLE_MV2PL_DRWLOCK)
          ; // do nothing. everything is done by validate function
#else
          // not silo. send out release messages
          mp_mark_ready(hash_table, s, octx->target, ctask->tid,
              opids ? opids[nops] : 0, octx->e, octx->optype);
#endif
        }

        break;

      case OPTYPE_INSERT:
        // should never get inserts to item table
        assert((octx->e->key & TID_MASK) != ITEM_TID);

        // only single mode supported now
        assert(mode == TXN_SINGLE);

        // we should have not allocated a copy for inserts
        assert(octx->data_copy == NULL);

        if (octx->target == s) {
#if SHARED_EVERYTHING
          selock_release(p, octx);
#else
#if ENABLE_WAIT_DIE_CC
          wait_die_release(s, p, s, ctask->tid, opids ? opids[nops] : 0, octx->e);
#elif ENABLE_BWAIT_CC
          bwait_release(s, p, s, ctask->tid, opids ? opids[nops] : 0, octx->e);
#elif ENABLE_NOWAIT_CC
          no_wait_release(p, octx->e);
#elif defined(ENABLE_SILO_CC) || defined(ENABLE_MV2PL) || defined(ENABLE_SVDREADLOCK_CC) || defined(ENABLE_MVDREADLOCK_CC) || defined(ENABLE_MV2PL_DRWLOCK)
          ; // do nothing. everything is done by validate function
#elif ENABLE_DL_DETECT_CC
          dl_detect_release(s, p, s, ctask->tid, opids ? opids[nops] : 0, octx->e, release_notify);
#elif !defined(SHARED_EVERYTHING) && !defined(SHARED_NOTHING)
#error "No CC algorithm specified"
#endif //ENABLE_WAIT_DIE_CC

#endif //SHARED_EVERYTHING

          /* selock or wait_die/no_wiat will reset ref count once
           * if we are aborting, we reset it again. This will delete
           * the element and free up space
           */
          if (status == TXN_ABORT) {
            assert(octx->e->ref_count == 1);
            mp_release_value_(p, octx->e);
          }

        } else {
          // XXX: If we need to abort a remote insert, we need a new
          // HASHOP_DELETE that we don't support yet.
          // check if we need to do this

              assert(0);


          //mp_mark_ready(hash_table, s, octx->target, ctask->tid, 0, octx->e);
        }

        break;
    }
  }
#if (defined(SHARED_EVERYTHING) && defined(ENABLE_DL_DETECT_CC))
//  DL_detect_clear_dep(p, &dl_detector, ctask->g_tid);
//  DL_detect_remove_dep(&dl_detector, ctask->g_tid);
  struct se_dl_detect_graph_node src;
  src.srvfib = ctask->g_tid;
  src.ts = ctx->ts;
  se_dl_detect_clear_dependencies(&src, status);
  dprint("Server %d finishing\n", s);
#endif

#if !defined(SHARED_EVERYTHING) && !defined(SHARED_NOTHING)
  smp_flush_all(hash_table, s);
#endif

  ctx->nops = 0;

  if (status == TXN_COMMIT) {
      p->ncommits++;
      if (ctx->op_ctx[0].optype == OPTYPE_LOOKUP) {
          p->ncommits_ronly++;
      }
      else
          p->ncommits_wonly++;

  } else {
      p->naborts++;

      if (ctx->op_ctx[0].optype == OPTYPE_LOOKUP) {
          p->naborts_ronly++;
      }
      else
          p->naborts_wonly++;
  }

  return status;
}

void txn_abort(struct task *ctask, struct hash_table *hash_table, int s, int mode)
{
#if SHARED_NOTHING
  assert(0);
#endif

  txn_finish(ctask, hash_table, s, TXN_ABORT, mode, NULL);
}

int txn_commit(struct task *ctask, struct hash_table *hash_table, int s, int mode)
{
#if SHARED_NOTHING
  assert(0);
#endif

  return txn_finish(ctask, hash_table, s, TXN_COMMIT, mode, NULL);
}

void process_requests(struct hash_table *hash_table, int s)
{
  struct box_array *boxes = hash_table->boxes;
  uint64_t inbuf[ONEWAY_BUFFER_SIZE];
  struct partition *p = &hash_table->partitions[s];
  int s_coreid = hash_table->thread_data[s].core;
  char skip_list[BITNSLOTS(MAX_SERVERS)];

  struct req {
    struct elem *e;
    uint64_t optype;
    int tid;
    int opid;
    uint64_t ts;
    int r;
#if ENABLE_DL_DETECT_CC
    int s;
#endif
  } reqs[ONEWAY_BUFFER_SIZE];

  int nreqs;

#if ENABLE_DL_DETECT_CC
  int all_reqs = 0;
  int notification = 0;
#endif

#if ENABLE_SOCKET_LOCAL_TXN
  memset(skip_list, 0, sizeof(skip_list));

  /* Having cores check msg buffer incurs overhead in the cross socket case
   * as cache coherence enforcement causes snooping. So to minimize the
   * overhead, we have a mode where we do only socket local txns. This can
   * be easily enforced at runtime by using a scheduler that queues a txn
   * in same socket as thread that has the data.
   *
   * XXX: This requires determining for a given core, all others in socket.
   * This can be determined programatically. For now, we hack it in and
   * make it specific to diascld33 -- the 4 socket server where mapping is
   * 0,4,8,...68: s0
   * 1,5,9,...69: s1
   * 2,6,10,...70: s3
   * 3,7,11,...71: s4
   *
   * s will range from 0 to nservers. Given s, s/18 is socketid
   * on which this server sits. Then, based on above mapping min clientid is
   * socketid * 18 the max clientid in that socket is min + 18.
   *
   */

  /* check only socket-local guys plus first core on other sockets
   * Enforce this by setting appropriate bits in the skip list
   */
  for (int i = 0; i < g_nservers; i++) {
      int t_coreid = hash_table->thread_data[i].core;

      // check only cores in our own socket of leader cores in other sockets
      if (s_coreid % 4 == t_coreid % 4 || t_coreid < 4) {
        ;
      } else {
        BITSET(skip_list, i);
      }
  }
#endif

  for (int i = 0; i < g_nservers; i++) {
    if (i == s)
      continue;

#if ENABLE_SOCKET_LOCAL_TXN
    if (BITTEST(skip_list, i)) {
      continue;
    }
#endif

#if ENABLE_DL_DETECT_CC
    int abt_srv = -1;
#endif


    struct onewaybuffer *b = &boxes[i].boxes[s].in;
    int count = b->wr_index - b->rd_index;
    if (count == 0)
      continue;

    /* We can get many requests from one server as there can be many tasks.
     * We process it as follows
     * 1) get all requests and retrieve all elements
     * 2) group elements into sets, one set per unique task
     * 3) process task groups one at a time.
     *
     * Within task group, there are 3 cases for each request depending on CC:
     * 1) abort 2) success 3) wait
     * 1) Even if 1 request aborts, whole txn will abort. So no point in sending
     * back any other request. So just fail all requests
     * 2) if success, send back data to caller
     * 3) if wait, don't do anything. data will be sent back later
     */
    count = buffer_read_all(b, ONEWAY_BUFFER_SIZE, inbuf, 0);
    assert(count);

    dprint("srv(%d): read %d messages from client %d\n", s, count, i);

    // get all elems
    int j = 0;
    nreqs = 0;
    while (j < count) {
      uint64_t optype = inbuf[j] & HASHOP_MASK;

      switch(optype) {
          case HASHOP_MIGRATE:
          {
              struct task *task_pointer = (struct task*)(HASHOP_GET_VAL(inbuf[j]));
              task_resume_migration(task_pointer, p);
              j += MIGRATE_MSG_LENGTH;
              break;
          }
        case HASHOP_RD_RELEASE:
        case HASHOP_WT_RELEASE:
        case HASHOP_CERT_RELEASE:
        case HASHOP_RELEASE:
        {
          struct elem *t = (struct elem *)(HASHOP_GET_VAL(inbuf[j]));
          short tid = HASHOP_GET_TID(inbuf[j]);
          short opid = HASHOP_GET_OPID(inbuf[j]);

          dprint("srv(%d): cl %d before release %" PRIu64 " rc %" PRIu64 "\n",
              s, i, t->key, t->ref_count);

#if ENABLE_WAIT_DIE_CC
          wait_die_release(s, p, i, tid, opid, t);
#elif ENABLE_BWAIT_CC
          bwait_release(s, p, i, tid, opid, t);
#elif ENABLE_NOWAIT_CC
          no_wait_release(p, t);
#elif ENABLE_SILO_CC
          bwait_release(s, p, i, tid, opid, t);

          // bwait will clear the lock entry for previous owner. if there is a
          // waiter who is waiting, it becomes the new owner. so its lock entry
          // moves from the waiters list to the owners list. in this case, the
          // tid lock bit must remain set. but if there is no owner, we need to
          // clear the tid bit
          if (!(t->ref_count & DATA_READY_MASK))
              t->tid = t->tid & ~SILO_LOCK_BIT;
#elif ENABLE_DL_DETECT_CC
          dl_detect_release(s, p, i, tid, opid, t, 1);
#elif ENABLE_MV2PL
          del_mv2pl_release(p, t, optype == HASHOP_RD_RELEASE ? OPTYPE_LOOKUP :
                  (optype == HASHOP_WT_RELEASE ? OPTYPE_UPDATE :
                       OPTYPE_CERTIFY));
#elif !defined(SHARED_EVERYTHING) && !defined(SHARED_NOTHING)
#error "No CC algorithm specified"
#endif

          j += RELEASE_MSG_LENGTH;
          break;
        }
        case HASHOP_INSERT:
        {
          hash_key key = HASHOP_GET_VAL(inbuf[j]);
          short tid = HASHOP_GET_TID(inbuf[j]);
          short opid = HASHOP_GET_OPID(inbuf[j]);
          size_t sz = inbuf[j + 1];

          dprint("srv(%d): cl %d inserting key %"PRIu64" sz %d\n",
              s, i, key, sz);

          struct elem *e = hash_insert(p, key, sz, NULL);
          assert(e);

          e->ref_count = 1;
          p->ninserts++;

          int r;
#if ENABLE_WAIT_DIE_CC
          struct lock_entry *l;

          r = wait_die_acquire(s, p, i, tid, opid, e, OPTYPE_INSERT,
              inbuf[j + 2], &l);

#elif ENABLE_BWAIT_CC
          struct lock_entry *l;

          r = bwait_acquire(s, p, i, tid, opid, e, OPTYPE_INSERT, &l);
#elif ENABLE_NOWAIT_CC
          r = no_wait_acquire(e, OPTYPE_INSERT);
#elif ENABLE_SILO_CC
          assert(0);
#elif ENABLE_DL_DETECT_CC
          struct lock_entry *l;

		  r = dl_detect_acquire(s, p, i, tid, opid, e, OPTYPE_INSERT, &l, inbuf[j + 2], &notification);
#elif ENABLE_MV2PL
          assert(0);
#elif !defined(SHARED_EVERYTHING) && !defined(SHARED_NOTHING)
#error "No CC algorithm specified"
#endif

          // insert can only succeed
          assert (r == LOCK_SUCCESS);

          // reply back
          uint64_t out_msg = MAKE_HASH_MSG(tid, opid, (unsigned long)e, 0);
          buffer_write_all(&boxes[i].boxes[s].out, 1, &out_msg, 0);

          j += INSERT_MSG_LENGTH;

          break;
        }
        case HASHOP_LOOKUP:
        case HASHOP_UPDATE:
        case HASHOP_CERTIFY:
        {
          hash_key key = HASHOP_GET_VAL(inbuf[j]);
          struct req *req = &reqs[nreqs];
          req->optype = optype;
          req->tid = HASHOP_GET_TID(inbuf[j]);
          req->opid = HASHOP_GET_OPID(inbuf[j]);
          req->r = LOCK_INVALID;
#if ENABLE_DL_DETECT_CC
          reqs[nreqs].s = i;
          req->ts = inbuf[j + 1];
#endif
#if ENABLE_WAIT_DIE_CC
          req->ts = inbuf[j + 1];
#endif

          dprint("srv (%d): cl %d tid %d opid %d %s %" PRIu64 "\n", s, i,
              req->tid, req->opid,
              OPTYPE_STR(optype), key);

          req->e = hash_lookup(p, key);
          if (!req->e) {
            printf("srv (%d): cl %d %s %" PRIu64 " failed\n", s, i,
                OPTYPE_STR(optype), key);
          }
          assert(req->e);

          nreqs++;
          j += LOOKUP_MSG_LENGTH;
          break;
        }
#if ENABLE_DL_DETECT_CC
        case DL_DETECT_ABT_TXN:
        {
        	struct req *req = &reqs[nreqs];
			reqs[nreqs].s = HASHOP_GET_OPID(inbuf[j]);
			req->optype = DL_DETECT_ABT_TXN;
			req->tid = HASHOP_GET_TID(inbuf[j]) + 2;
			req->opid = HASHOP_TSMSG_GET_OPID(inbuf[j + 1]);
			req->ts = HASHOP_TSMSG_GET_TS(inbuf[j + 1]);
			reqs[nreqs].e = (struct elem *)(HASHOP_GET_VAL(inbuf[j]));
			req->r = LOCK_ABORT;
			abt_srv = HASHOP_GET_OPID(inbuf[j]);
			nreqs++;
			j += 2;
			dprint("srv(%d) will abort the txn of srv %d fiber %d key %ld after receiving a msg from srv %d\n", s, abt_srv, req->tid, req->e->key, i);
			break;
        }
#endif
        default:
        {
          printf("cl %d invalid message type %lx\n", i, optype);
          printf("Received msg with optype %ld\n", optype);
          fflush(stdout);
          assert(0);
          break;
        }
      }
    }

#if !defined(MIGRATION)
    // do trial task at a time
    for (j = 0; j < nreqs; j++) {
      struct req *req = &reqs[j];

      assert(req->optype != HASHOP_RELEASE);
#if ENABLE_DL_DETECT_CC
      assert(req->optype == HASHOP_LOOKUP || req->optype == HASHOP_UPDATE || req->optype == DL_DETECT_ABT_TXN);
#else
      assert(req->optype == HASHOP_LOOKUP || req->optype == HASHOP_UPDATE);
#endif

      // if someone marked this as abort, don't do req
      if (req->r != LOCK_INVALID) {
        continue;
      }

#if ENABLE_WAIT_DIE_CC
      req->r = wait_die_check_acquire(s, p, i, req->tid, req->opid, req->e,
          req->optype == HASHOP_LOOKUP ? OPTYPE_LOOKUP : OPTYPE_UPDATE,
          req->ts);
#elif ENABLE_BWAIT_CC
      req->r = bwait_check_acquire(req->e,
          req->optype == HASHOP_LOOKUP ? OPTYPE_LOOKUP : OPTYPE_UPDATE);
#elif ENABLE_NOWAIT_CC
      req->r = no_wait_check_acquire(req->e,
          req->optype == HASHOP_LOOKUP ? OPTYPE_LOOKUP : OPTYPE_UPDATE);
#elif ENABLE_SILO_CC
      //silo sends requests only during validation and only update reqs
      assert(req->optype == HASHOP_UPDATE);

      req->r = bwait_check_acquire(req->e, OPTYPE_UPDATE);
#elif ENABLE_DL_DETECT_CC
      req->r = dl_detect_check_acquire(req->e,
          	  req->optype == HASHOP_LOOKUP ? OPTYPE_LOOKUP : OPTYPE_UPDATE);
#elif ENABLE_MV2PL
      req->r = del_mv2pl_check_acquire(req->e,
          	  req->optype == HASHOP_LOOKUP ? OPTYPE_LOOKUP : OPTYPE_UPDATE);
#elif !defined(SHARED_EVERYTHING) && !defined(SHARED_NOTHING)
#error "No CC algorithm specified"
#endif

      char abort = 0;

      if (req->r == LOCK_ABORT) {
#if defined(ENABLE_BWAIT_CC) || defined(ENABLE_SILO_CC) || defined(ENABLE_DL_DETECT_CC)
        assert(0);
#endif
        abort = 1;
        dprint("srv (%d): cl %d tid %d opid %d %s %" PRIu64 " aborted\n",
            s, i, req->tid, req->opid,
            req->optype == HASHOP_LOOKUP ? "lookup" : "update", req->e->key);
      }

      /* now try acquring locks of all other requests from same task
       * in case j aborted, set all others to abort as well
       */
      for (int k = j + 1; k < nreqs; k++) {
        if (req->tid == reqs[k].tid) {
          assert(req->e != reqs[k].e);

          if (req->r == LOCK_ABORT) {
            reqs[k].r = LOCK_ABORT;
          } else {
            assert (req->r != LOCK_INVALID);

#if ENABLE_WAIT_DIE_CC
            reqs[k].r = wait_die_check_acquire(s, p, i, reqs[k].tid,
                reqs[k].opid, reqs[k].e,
                reqs[k].optype == HASHOP_LOOKUP ? OPTYPE_LOOKUP : OPTYPE_UPDATE,
                reqs[k].ts);
#elif ENABLE_BWAIT_CC
            reqs[k].r = bwait_check_acquire(reqs[k].e,
                reqs[k].optype == HASHOP_LOOKUP ? OPTYPE_LOOKUP : OPTYPE_UPDATE);
#elif ENABLE_NOWAIT_CC
            reqs[k].r = no_wait_check_acquire(reqs[k].e,
                reqs[k].optype == HASHOP_LOOKUP ? OPTYPE_LOOKUP : OPTYPE_UPDATE);
#elif ENABLE_SILO_CC
            reqs[k].r = bwait_check_acquire(reqs[k].e, OPTYPE_UPDATE);
#elif ENABLE_DL_DETECT_CC
            reqs[k].r = dl_detect_check_acquire(reqs[k].e,
				 reqs[k].optype == HASHOP_LOOKUP ? OPTYPE_LOOKUP : OPTYPE_UPDATE);
#elif ENABLE_MV2PL
            reqs[k].r = del_mv2pl_check_acquire(reqs[k].e,
				 reqs[k].optype == HASHOP_LOOKUP ? OPTYPE_LOOKUP : OPTYPE_UPDATE);
#elif !defined(SHARED_EVERYTHING) && !defined(SHARED_NOTHING)
#error "No CC algorithm specified"
#endif

            if (reqs[k].r == LOCK_ABORT) {
#if defined(ENABLE_BWAIT_CC) || defined(ENABLE_SILO_CC) || defined(ENABLE_DL_DETECT_CC)
                assert(0);
#endif
              req->r = LOCK_ABORT;
              abort = 1;
            }
          }
        }
      }

      if (!abort) {
        //actually acquire the locks for all requests from j's task
        for (int k = j; k < nreqs; k++) {
          if (reqs[j].tid != reqs[k].tid)
            continue;

          assert(reqs[k].r != LOCK_INVALID);

          int res;

#if ENABLE_WAIT_DIE_CC
          struct lock_entry *l = NULL;

          res = wait_die_acquire(s, p, i, reqs[k].tid,
              reqs[k].opid, reqs[k].e,
              reqs[k].optype == HASHOP_LOOKUP ? OPTYPE_LOOKUP : OPTYPE_UPDATE,
              reqs[k].ts, &l);
#elif ENABLE_BWAIT_CC
          struct lock_entry *l = NULL;

          res = bwait_acquire(s, p, i, reqs[k].tid,
              reqs[k].opid, reqs[k].e,
              reqs[k].optype == HASHOP_LOOKUP ? OPTYPE_LOOKUP : OPTYPE_UPDATE,
              &l);
#elif ENABLE_NOWAIT_CC
          res = no_wait_acquire(reqs[k].e, reqs[k].optype == HASHOP_LOOKUP ?
              OPTYPE_LOOKUP : OPTYPE_UPDATE);
#elif ENABLE_SILO_CC
          struct lock_entry *l = NULL;

          res = bwait_acquire(s, p, i, reqs[k].tid,
              reqs[k].opid, reqs[k].e, OPTYPE_UPDATE,
              &l);

          if (res == LOCK_SUCCESS) {
              // tid bit must have been clear
              assert(!(reqs[k].e->tid & SILO_LOCK_BIT));

              reqs[k].e->tid |= SILO_LOCK_BIT;
          }
#elif ENABLE_DL_DETECT_CC
          struct lock_entry *l = NULL;

		  res = dl_detect_acquire(s, p, i, reqs[k].tid,
			  reqs[k].opid, reqs[k].e,
			  reqs[k].optype == HASHOP_LOOKUP ? OPTYPE_LOOKUP : OPTYPE_UPDATE,
			  &l, reqs[k].ts, &notification);

#elif ENABLE_MV2PL
          res = del_mv2pl_acquire(reqs[k].e, reqs[k].optype == HASHOP_LOOKUP ?
                  OPTYPE_LOOKUP : (reqs[k].optype == HASHOP_UPDATE ?
                      OPTYPE_UPDATE : OPTYPE_CERTIFY));

#elif !defined(SHARED_EVERYTHING) && !defined(SHARED_NOTHING)
#error "No CC algorithm specified"
#endif
#if ENABLE_DL_DETECT_CC
		  assert(res == LOCK_ABORT || res == reqs[k].r);
		  reqs[k].r = res;
#else
          assert(res == reqs[k].r);
#endif
        }

      } else {
        assert(req->r == LOCK_ABORT);

        // if any of k failed, fail all of them
        for (int k = j + 1; k < nreqs; k++) {
          if (req->tid == reqs[k].tid)
            req[k].r = LOCK_ABORT;
        }
      }
    }

    // now send back the responses
    for (j = 0; j < nreqs; j++) {
      uint64_t out_msg;
      struct req *req = &reqs[j];
      int r = req->r;

      // skip all release messages
      assert (req->optype != HASHOP_RELEASE);

      assert(r != LOCK_INVALID);

      /* now, skip waits, respond back for success and aborts and get the locks
       * for real
       */
      if (r == LOCK_ABORT) {
        out_msg = MAKE_HASH_MSG(req->tid, req->opid, 0, 0);
#if 0//ENABLE_DL_DETECT_CC
        /* First, remove the thread which issued the transaction
         * from the owners and the waiters list for that key.
         * Then, send the message to the thread, which issued the txn.
         */
        if (s != req->s) {
        	dl_detect_release(s, p, req->s, req->tid, req->opid, req->e, 0);
        	buffer_write_all(&boxes[req->s].boxes[s].out, 1, &out_msg, 1);
			dprint("SRV %d sent LOCK ABORT to SRV %d FIB %d KEY %"PRIu64"; msg = %ld\n", s, req->s, req->tid, req->e->key, out_msg);
        } else {
        	struct lock_entry *le;
        	TAILQ_FOREACH(le, &req->e->waiters, next) {
        		dprint("srv(%d): Checking entry (%d,%d,%ld) with (%d,%d,%ld)\n", s,
						le->s, le->task_id, le->ts,
						req->s, req->tid, req->ts);
				if ((le->s == req->s) && (le->task_id == req->tid)) {
					le->ready = LOCK_ABORT_NXT;
					*(le->notify) = 1;
					break;
				}
        	}
        	assert(le != NULL);
        	dprint("SRV %d ABORTING local txn for FIB %d KEY %"PRIu64"; msg = %ld\n", s, req->tid, req->e->key, out_msg);
        }
#else
        buffer_write_all(&boxes[i].boxes[s].out, 1, &out_msg, 0);
#endif
      } else if (r == LOCK_SUCCESS) {
        out_msg = MAKE_HASH_MSG(req->tid, req->opid, (unsigned long)req->e, 0);
        buffer_write_all(&boxes[i].boxes[s].out, 1, &out_msg, 0);
      } else {
        assert (r == LOCK_WAIT);
      }
    }

    buffer_flush(&boxes[i].boxes[s].out);
#if ENABLE_DL_DETECT_CC
    all_reqs += nreqs;
#endif
#endif
  }
}
#if 0
void *hash_table_server(void* args)
{
  int i, r;
  const int s = ((struct thread_args *) args)->id;
  const int c = ((struct thread_args *) args)->core;
  struct hash_table *hash_table = ((struct thread_args *) args)->hash_table;
  struct partition *p = &hash_table->partitions[s];
  void *query;
  __attribute__((unused)) int pct = 10;

  set_affinity(c);
#if defined(MIGRATION)
  fix_affinity(s);
#endif

  double tstart = now();

#if SHARED_EVERYTHING
  /* load only one partition in case of shared everything */
  //if (s == 0)
    g_benchmark->load_data(hash_table, s);

#else

#if ENABLE_ASYMMETRIC_MESSAGING
  /* load only few partitions in case of asym msg. */
  if (s < g_nhot_servers)
      g_benchmark->load_data(hash_table, s);
#else
#if 0//ENABLE_DL_DETECT_CC

// server N is the deadlock detector
 if (s < g_nservers - 1)
	g_benchmark->load_data(hash_table, s);
#else
  /* always load for sn/trireme */
  g_benchmark->load_data(hash_table, s);
#endif //ENABLE_DL_DETECT_CC

#endif //ENABLE_ASYMMETRIC_MESSAGING

#endif //SHARED_EVERYTHING

  double tend = now();

  printf("srv %d load time %.3f\n", s, tend - tstart);
  printf("srv %d rec count: %d partition sz %lu-KB "
      "tx count: %d, per_txn_op cnt: %d\n", s, p->ninserts, p->size / 1024,
      g_niters, g_ops_per_txn);

  pthread_mutex_lock(&hash_table->create_client_lock);

  nready++;

  pthread_mutex_unlock(&hash_table->create_client_lock);

  while (nready != g_active_servers) ;

  printf("srv %d starting txns\n", s);
  fflush(stdout);

  query = g_benchmark->alloc_query();
  assert(query);

  tstart = now();

#if ENABLE_ASYMMETRIC_MESSAGING

#if defined(SHARED_EVERYTHING) || defined(SHARED_NOTHING)
#error "Asymmetric messaging valid only in msgpassing mode\n"
#endif

  if (s >= g_nhot_servers)
      task_libinit(s);
#elif 0//!defined(SHARED_EVERYTHING) && defined(ENABLE_DL_DETECT_CC)
  if (s < g_nservers - 1)
	  task_libinit(s);
  else {
	  dl_detect_fn(s);
  }
#else
  task_libinit(s);
#endif

  tend = now();

  printf("srv %d query time %.3f\n", s, tend - tstart);
  printf("srv %d total txns %d \n", s, p->q_idx);
  printf("srv %d commited txns %d aborted %d\n", s, p->ncommits, p->naborts);

  fflush(stdout);

#if ENABLE_ASYMMETRIC_MESSAGING
  if (s < g_nhot_servers)
      p->tps = 0;
  else
      p->tps = p->q_idx / (tend - tstart);
#else
  p->tps = p->q_idx / (tend - tstart);
#endif

  pthread_mutex_lock(&hash_table->create_client_lock);

  nready--;

  pthread_mutex_unlock(&hash_table->create_client_lock);

  while (nready != 0)
#if !defined (SHARED_EVERYTHING) && !defined (SHARED_NOTHING)
#if 0//ENABLE_DL_DETECT_CC
	  if (s < g_nservers - 1)
#endif
    process_requests(hash_table, s);
#else
    ;
#endif

  printf("srv %d quitting \n", s);
  fflush(stdout);

  if (g_benchmark->verify_txn)
    g_benchmark->verify_txn(hash_table, s);

  // destory out dses
#if !defined (SHARED_EVERYTHING)
  /* in shared nothing and trireme case, we can safely delete
   * the partition now as no one else will be directly accessing
   * data from the partition. In shared everything case, some other
   * thread might still be doing verification. So we don't destroy now.
   */
  destroy_hash_partition(p);
#endif

 #if 0
  // stay running until someone else needs us
  while (quitting == 0) {
    quitting = hash_table->quitting;
    process_requests(hash_table, s);
  }
#endif

  return NULL;
}
#endif

int smp_hash_lookup(struct task *ctask, struct hash_table *hash_table,
    int client_id, int server, hash_key key, short op_id)
{
  uint64_t msg_data[2];

  assert(server >= 0 && server < g_nservers);

  dprint("srv(%d): sending lookup for key %"PRIu64" to srv %d\n", client_id, key, server);

  msg_data[0] = MAKE_HASH_MSG(ctask->tid, op_id, key, HASHOP_LOOKUP);

#if ENABLE_WAIT_DIE_CC || ENABLE_DL_DETECT_CC
        // send timestamp as a payload in case of wait die cc
  msg_data[1] = ctask->txn_ctx.ts;
  buffer_write_all(&hash_table->boxes[client_id].boxes[server].in, 2, msg_data, 0);
#else
  buffer_write_all(&hash_table->boxes[client_id].boxes[server].in, 1, msg_data, 0);
#endif

  return 1;
}

#if 0
int smp_hash_insert(struct hash_table *hash_table, int client_id, hash_key key, int size)
{
  uint64_t msg_data[INSERT_MSG_LENGTH];
  int s = g_benchmark->hash_get_server(hash_table, key);

  msg_data[0] = (unsigned long)key | HASHOP_INSERT;
  msg_data[1] = (unsigned long)size;

  dprint("srv(%d): sending insert for key %"PRIu64" to srv %d\n", client_id, key, s);

  buffer_write_all(&hash_table->boxes[client_id].boxes[s].in, INSERT_MSG_LENGTH, msg_data, 0);

  return 1;
}
#endif

int smp_hash_update(struct task *ctask, struct hash_table *hash_table,
    int client_id, int server, hash_key key, short op_id)
{
  uint64_t msg_data[2];

  assert(server >= 0 && server < g_nservers);

  dprint("srv(%d): sending update for key %"PRIu64" to srv %d\n", client_id,
      key, server);

  msg_data[0] = MAKE_HASH_MSG(ctask->tid, op_id, key, HASHOP_UPDATE);

#if ENABLE_WAIT_DIE_CC || ENABLE_DL_DETECT_CC
        // send timestamp as a payload in case of wait die cc
  msg_data[1] = ctask->txn_ctx.ts;
  buffer_write_all(&hash_table->boxes[client_id].boxes[server].in, 2, msg_data, 0);
#else
  buffer_write_all(&hash_table->boxes[client_id].boxes[server].in, 1, msg_data, 0);
#endif

  return 1;
}

void smp_hash_doall(struct task *ctask, struct hash_table *hash_table,
    int client_id, int s, int nqueries, struct hash_op **queries,
    void **values, int opid)
{
  int r, i;
  uint64_t val;

  struct box_array *boxes = hash_table->boxes;
  uint64_t msg_data[3];

  for(int i = 0; i < nqueries; i++) {

    assert(i == 0);

    switch (queries[i]->optype) {
      case OPTYPE_LOOKUP:
        //s = g_benchmark->hash_get_server(hash_table, queries[i]->key);

        dprint("srv(%d): issue remote lookup op key %" PRIu64 " to %d\n",
          client_id, queries[i]->key, s);

        int s_coreid = hash_table->thread_data[s].core;
        int c_coreid = hash_table->thread_data[client_id].core;

        msg_data[0] = MAKE_HASH_MSG(ctask->tid, opid, queries[i]->key,
            HASHOP_LOOKUP);

#if ENABLE_WAIT_DIE_CC || ENABLE_DL_DETECT_CC
        // send timestamp as a payload in case of wait die cc
        msg_data[1] = ctask->txn_ctx.ts;
        buffer_write_all(&boxes[client_id].boxes[s].in, 2, msg_data, 0);
#else
        buffer_write_all(&boxes[client_id].boxes[s].in, 1, msg_data, 0);
#endif

#if GATHER_STATS
        hash_table->partitions[client_id].nlookups_remote++;
#endif
        break;
      case OPTYPE_UPDATE:
        //s = g_benchmark->hash_get_server(hash_table, queries[i]->key);

        dprint("srv(%d): issue remote update op key %" PRIu64 " to %d\n",
          client_id, queries[i]->key, s);

        msg_data[0] = MAKE_HASH_MSG(ctask->tid, opid, queries[i]->key,
            HASHOP_UPDATE);

#if ENABLE_WAIT_DIE_CC || ENABLE_DL_DETECT_CC
        msg_data[1] = ctask->txn_ctx.ts;
        buffer_write_all(&boxes[client_id].boxes[s].in, 2, msg_data, 0);
#else
        buffer_write_all(&boxes[client_id].boxes[s].in, 1, msg_data, 0);
#endif

#if GATHER_STATS
        hash_table->partitions[client_id].nupdates_remote++;
#endif
        break;

       case OPTYPE_CERTIFY:

        dprint("srv(%d): issue remote certify op key %" PRIu64 " to %d\n",
          client_id, queries[i]->key, s);

        msg_data[0] = MAKE_HASH_MSG(ctask->tid, opid, queries[i]->key,
            HASHOP_CERTIFY);

        buffer_write_all(&boxes[client_id].boxes[s].in, 1, msg_data, 0);

        break;

      case OPTYPE_INSERT:
        //s = g_benchmark->hash_get_server(hash_table, queries[i]->key);

        dprint("srv(%d): issue remote insert op key %" PRIu64 " to %d\n",
          client_id, queries[i]->key, s);

        msg_data[0] = MAKE_HASH_MSG(ctask->tid, opid, queries[i]->key,
            HASHOP_INSERT);

        msg_data[1] = queries[i]->size;

#if ENABLE_WAIT_DIE_CC || ENABLE_DL_DETECT_CC
        msg_data[2] = ctask->txn_ctx.ts;
        buffer_write_all(&boxes[client_id].boxes[s].in, 3, msg_data, 0);
#else
        buffer_write_all(&boxes[client_id].boxes[s].in, 2, msg_data, 0);
#endif

#if GATHER_STATS
        hash_table->partitions[client_id].nupdates_remote++;
#endif
        break;

      default:
        assert(0);
        break;
    }
  }

  ctask->npending = nqueries;
  ctask->nresponses = 0;

  // after queueing all the queries we flush all buffers and read all remaining values
  smp_flush_all(hash_table, client_id);

  task_yield(&hash_table->partitions[client_id], TASK_STATE_WAITING);

  // we should have data when we return
  assert(ctask->npending == ctask->nresponses);

  i = 0;
  while (i < nqueries) {
    int ps;

    if (queries[i]->optype == OPTYPE_PLOCK_ACQUIRE) {
      ps = queries[i]->key;
    } else {
      //ps = g_benchmark->hash_get_server(hash_table, queries[i]->key);
      ps = s;
    }

    val = ctask->received_responses[i];

    assert(HASHOP_GET_TID(val) == ctask->tid);
    assert(HASHOP_GET_OPID(val) == opid);

    values[i] = (void *)(unsigned long)HASHOP_GET_VAL(val);
    i++;

    //process_requests(hash_table, client_id);
  }
}

void smp_flush_all(struct hash_table *hash_table, int client_id)
{
  for (int i = 0; i < g_nservers; i++) {
    /* printf("srv(%d): flushing buffer %d rd count %d wcount %d twcount %d\n",
        client_id, i, hash_table->boxes[client_id].boxes[i].in.rd_index,
        hash_table->boxes[client_id].boxes[i].in.wr_index,
        hash_table->boxes[client_id].boxes[i].in.tmp_wr_index);
    */

    buffer_flush(&hash_table->boxes[client_id].boxes[i].in);
  }
}

/**
 * Value Memory Management Operations
 */
inline int is_value_ready(struct elem *e)
{
  return (e->ref_count & DATA_READY_MASK) == 0 ? 1 : 0;
}

void mp_release_value_(struct partition *p, struct elem *e)
{
  e->ref_count = (e->ref_count & (~DATA_READY_MASK)) - 1;

  dprint("srv(%ld): Releasing key %" PRIu64 " rc %" PRIu64 "\n",
      p - hash_table->partitions, e->key,
      e->ref_count);

  if (e->ref_count == 0) {
    //printf("key %" PRIu64 " 0 rc\n", e->key);
    assert(g_benchmark != &micro_bench);
    hash_remove(p, e);
  }
}

void mp_send_release_msg_(struct hash_table *hash_table, int client_id,
    int s, int task_id, int op_id, void *ptr, uint64_t hashop, int force_flush)
{
  struct elem *e = (struct elem *)ptr;

#if defined(SHARED_EVERYTHING) || defined(SHARED_NOTHING)
  assert(0);
#endif

  assert (s < g_nservers && client_id < g_nservers);

  // XXX: we are exploiting the 48-bit address here.
  uint64_t msg_data = MAKE_HASH_MSG(task_id, op_id, (uint64_t)e, hashop);
  assert(((struct elem *)HASHOP_GET_VAL(msg_data)) == e);

  dprint("srv(%ld): sending release msg %"PRIu64" for key %" PRIu64
      " rc %" PRIu64 " to %d\n", client_id, msg_data, e->key, e->ref_count, s);

  buffer_write_all(&hash_table->boxes[client_id].boxes[s].in, 1, &msg_data,
          force_flush);

  /* printf("srv(%d): buffer %d post release rd count %d wcount %d twcount %d\n",
      client_id, s, hash_table->boxes[client_id].boxes[s].in.rd_index,
      hash_table->boxes[client_id].boxes[s].in.wr_index,
      hash_table->boxes[client_id].boxes[s].in.tmp_wr_index);
      */

}

void mp_mark_ready(struct hash_table *hash_table, int client_id, int target,
    int task_id, int op_id, void *ptr, char optype)
{
  dprint("srv(%d): sending release msg key %" PRIu64 " rc %" PRIu64 " \n",
      client_id, ((struct elem *)ptr)->key, ((struct elem*)ptr)->ref_count);

  uint64_t hashop;

  if (optype == OPTYPE_LOOKUP) {
      hashop = HASHOP_RD_RELEASE;
  } else if (optype == OPTYPE_UPDATE) {
      hashop = HASHOP_WT_RELEASE;
  } else {
      assert(optype == OPTYPE_CERTIFY);
      hashop = HASHOP_CERT_RELEASE;
  }

  mp_send_release_msg_(hash_table, client_id, target, task_id, op_id, ptr,
      hashop, 0);
}

void mp_release_plock(int s, int c)
{
  uint64_t msg_data = HASHOP_PLOCK_RELEASE;

  buffer_write_all(&hash_table->boxes[s].boxes[c].in, 1, &msg_data, 1);
}

void mp_send_reply(int s, int c, short task_id, short op_id, struct elem *e)
{
  if (s == c)
    return;

  uint64_t msg_data = MAKE_HASH_MSG(task_id, op_id, (unsigned long)e, 0);

  buffer_write_all(&hash_table->boxes[c].boxes[s].out, 1, &msg_data, 1);
}

/**
 * Hash Table Counters and Stats
 */
void stats_reset(struct hash_table *hash_table)
{
  for (int i = 0; i < g_nservers; i++) {
    hash_table->partitions[i].ninserts = 0;
    hash_table->partitions[i].nlookups_local = 0;
    hash_table->partitions[i].nupdates_local = 0;
    hash_table->partitions[i].naborts = 0;
    hash_table->partitions[i].naborts_ronly = 0;
    hash_table->partitions[i].naborts_wonly = 0;
    hash_table->partitions[i].nlookups_remote = 0;
    hash_table->partitions[i].nupdates_remote = 0;
  }
}

int stats_get_ncommits(struct hash_table *hash_table)
{
  int ncommits = 0, ncommits_ronly = 0, ncommits_wonly = 0;
  int nvalidate_success = 0, nvalidate_failure = 0;

  for (int i = 0; i < g_nservers; i++) {
    dprint("srv %d commits %d\n", i,
      hash_table->partitions[i].ncommits);

    ncommits += hash_table->partitions[i].ncommits;
    ncommits_ronly += hash_table->partitions[i].ncommits_ronly;
    ncommits_wonly += hash_table->partitions[i].ncommits_wonly;
    nvalidate_success += hash_table->partitions[i].nvalidate_success;
    nvalidate_failure += hash_table->partitions[i].nvalidate_failure;
  }

  printf("total commits %d, ronly %d, wonly %d\n", ncommits,
          ncommits_ronly, ncommits_wonly);

  printf("validate success %d, failure %d\n", nvalidate_success,
          nvalidate_failure);

  return ncommits;

}

#if GATHER_STATS
int stats_get_task_stats(struct hash_table *hash_table) {
    for (int i = 0; i < g_nservers; i++) {
        printf("Server %d scheduler run time: %f\n", i, hash_table->partitions[i].root_task.run_time);
        printf("Server %d unblock run time: %f\n", i, hash_table->partitions[i].unblock_task.run_time);

    }
    for (int i = 0; i < g_nservers; i++) {
        struct task *t;
        for (int j = FIRST_TASK_ID; j < FIRST_TASK_ID + g_nfibers; j++) {
            struct task *t = g_tasks[i][j];
            printf("Task %d number of times scheduled: %d\n", t->g_tid,  t->times_scheduled);
            printf("Task %d run time: %f\n", t->g_tid, t->run_time);
            printf("Task %d commit/abort counts: %10d/%10d\n", t->g_tid, t->ncommits, t->naborts);
        }
    }
}
#endif

int stats_get_latency(struct hash_table *hash_table)
{

    double total_latency = 0, min_latency = UINT64_MAX, max_latency = 0;
    uint64_t ntxns = 0;

    for (int i = 0; i < g_nservers; i++) {
        struct partition *p = &hash_table->partitions[i];
        total_latency += p->total_txn_latency;
        if (min_latency > p->min_txn_latency)
            min_latency = p->min_txn_latency;

        if (max_latency < p->max_txn_latency)
            max_latency = p->max_txn_latency;

        ntxns += p->q_idx;
    }

    printf("Total latency (secs) %f, ntxns %"PRIu64", Avg latency %f us,"
            "min latency %f us, max_latency %f us\n", total_latency, ntxns,
            total_latency * 1000000 / ntxns, min_latency * 1000000,
            max_latency * 1000000);

}

int stats_get_nlookups(struct hash_table *hash_table)
{
  int nlookups = 0;
  for (int i = 0; i < g_nservers; i++) {
    printf("srv %d lookups local: %d remote %d\n", i,
      hash_table->partitions[i].nlookups_local,
      hash_table->partitions[i].nlookups_remote);

    nlookups += hash_table->partitions[i].nlookups_local +
      hash_table->partitions[i].nlookups_remote;
  }

  printf("total lookups %d\n", nlookups);

  return nlookups;
}

int stats_get_nupdates(struct hash_table *hash_table)
{
  int nupdates = 0;
  for (int i = 0; i < g_nservers; i++) {
    printf("srv %d updates local: %d remote %d \n", i,
      hash_table->partitions[i].nupdates_local,
      hash_table->partitions[i].nupdates_remote);

    nupdates += hash_table->partitions[i].nupdates_local +
      hash_table->partitions[i].nupdates_remote;
  }

  printf("total updates %d\n", nupdates);

  return nupdates;
}

int stats_get_naborts(struct hash_table *hash_table)
{
  int naborts = 0, naborts_ronly = 0, naborts_wonly = 0;
  for (int i = 0; i < g_nservers; i++) {
    dprint("srv %d aborts %d \n", i,
      hash_table->partitions[i].naborts);

    naborts += hash_table->partitions[i].naborts;
    naborts_ronly += hash_table->partitions[i].naborts_ronly;
    naborts_wonly += hash_table->partitions[i].naborts_wonly;
  }

  printf("Total aborts: %d, ronly aborts %d, wonly aborts %d\n", naborts,
          naborts_ronly, naborts_wonly);

  return naborts;
}


int stats_get_ninserts(struct hash_table *hash_table)
{
  int ninserts = 0;
  for (int i = 0; i < g_nservers; i++) {
    printf("Server %d : %d tuples %lu-KB mem \n", i,
      hash_table->partitions[i].ninserts, hash_table->partitions[i].size / 1024);
    ninserts += hash_table->partitions[i].ninserts;
  }

  printf("total inserts %d\n", ninserts);

  return ninserts;
}

void stats_get_buckets(struct hash_table *hash_table, int server, double *avg, double *stddev)
{
  struct partition *p = &hash_table->partitions[server];

  int nelems = 0;
  struct elem *e;

  for (int i = 0; i < p->nhash; i++) {
    e = LIST_FIRST(&p->table[i].chain);
    while (e != NULL) {
      nelems++;
      e = LIST_NEXT(e, chain);
    }
  }
  *avg = (double)nelems / p->nhash;
  *stddev = 0;

  for (int i = 0; i < p->nhash; i++) {
    e = LIST_FIRST(&p->table[i].chain);
    int length = 0;
    while (e != NULL) {
      e = LIST_NEXT(e, chain);
      length++;
    }

    *stddev += (*avg - length) * (*avg - length);
  }

  *stddev = sqrt(*stddev / (nelems - 1));
}

void stats_get_mem(struct hash_table *hash_table, size_t *used, size_t *total)
{
  struct partition *p;
  size_t m = 0, u = 0;

  for (int i = 0; i < g_nservers; i++) {
    p = &hash_table->partitions[i];

    m += p->nrecs;
    u += p->size;
  }

  *total = m;
  *used = u;
}

double stats_get_tps(struct hash_table *hash_table)
{
  double tps = 0;

  for (int i = 0; i < g_nservers; i++) {
    printf("Server %d: %0.9fM TPS\n", i,
      (double)hash_table->partitions[i].tps / 1000000);

    tps += ((double)hash_table->partitions[i].tps / 1000000);
  }

  return tps;
}
