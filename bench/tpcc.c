#include <limits.h>
#include "headers.h"
#include "smphashtable.h"
#include "partition.h"
#include "benchmark.h"
#include "plmalloc.h"
#include "tpcc.h"
#define ACCESSIBLE_WAREHOUSES 1

#define CHK_ABORT(val) \
  if (!(val)) {\
    dprint("srv(%d): Aborting due to key %"PRId64"\n", id, pkey); \
    r = TXN_ABORT; \
    goto final; \
  }

struct item {
  int ol_i_id;
  int ol_supply_w_id;
  int ol_quantity;
};

// neworder tpcc query
struct tpcc_query {
  int w_id;
  int d_id;
  int c_id;
  int threshold;
  int o_carrier_id;
  int d_w_id;
  int c_w_id;
  int c_d_id;
  char c_last[LAST_NAME_LEN];
  double h_amount;
  char by_last_name;
  struct item item[TPCC_MAX_OL_PER_ORDER];
  char rbk;
  char remote;
  int ol_cnt;
  int o_entry_d;
};

/* fwd declarations */
static int fetch_cust_records(struct hash_table *hash_table, int id,
    struct task *ctask, struct tpcc_customer **c_recs,
    struct secondary_record *sr, struct tpcc_query *q,
    short *opids, short *nopids);

static int batch_fetch_cust_records(struct hash_table *hash_table, int id,
    struct task *ctask, struct tpcc_customer **c_recs,
    struct secondary_record *sr, struct tpcc_query *q,
    short *opids, short *nopids);

static int set_last_name(int num, char* name)
{
  static const char *n[] =
  {"BAR", "OUGHT", "ABLE", "PRI", "PRES",
    "ESE", "ANTI", "CALLY", "ATION", "EING"};

  strcpy(name, n[num/100]);
  strcat(name, n[(num/10)%10]);
  strcat(name, n[num%10]);
  return strlen(name);
}

static void tpcc_init()
{
#if !defined(SE_INDEX_LATCH) && defined(SHARED_EVERYTHING)
    printf("Index latching not defined for TPCC!\n");
    exit(1);
#endif
}

void tpcc_get_next_payment_query(struct hash_table *hash_table, int s,
    void *arg)
{
  struct partition *p = &hash_table->partitions[s];
  struct tpcc_query *q = (struct tpcc_query *) arg;

  q->w_id = (s) + 1;;
  q->d_w_id = (s)  + 1;;
  q->d_id = URand(&p->seed, 1, TPCC_NDIST_PER_WH);
  q->h_amount = URand(&p->seed, 1, 5000);
  int x = URand(&p->seed, 1, 100);
  int y = URand(&p->seed, 1, 100);

  if(x <= 85 || g_dist_threshold == 100) {
    // home warehouse
    q->c_d_id = q->d_id;
    q->c_w_id = (s) + 1;;
  } else {
    q->c_d_id = URand(&p->seed, 1, TPCC_NDIST_PER_WH);

    // remote warehouse if we have >1 wh
    if(g_nservers > 1) {
      while((q->c_w_id = URand(&p->seed, 1, g_nservers)) == (s + 1))
        ;

    } else {
      q->c_w_id = (s) + 1;;
    }
  }

  if (y <= 60) {
    // by last name
    q->by_last_name = TRUE;
    set_last_name(NURand(&p->seed, 255, 0, 999), q->c_last);
  } else {
    // by cust id
    q->by_last_name = FALSE;
    q->c_id = NURand(&p->seed, 1023, 1, TPCC_NCUST_PER_DIST);
  }
}

void tpcc_get_next_neworder_query(struct hash_table *hash_table, int s,
    void *arg)
{
  struct partition *p = &hash_table->partitions[s];
  int ol_cnt, dup;
  struct tpcc_query *q = (struct tpcc_query *) arg;

  q->w_id = (s) + 1;;
  //q->w_id = URand(&p->seed, 1, g_nservers);
  q->d_id = URand(&p->seed, 1, TPCC_NDIST_PER_WH);
  q->c_id = NURand(&p->seed, 1023, 1, TPCC_NCUST_PER_DIST);
  q->rbk = URand(&p->seed, 1, 100);
  q->ol_cnt = URand(&p->seed, 5, 15);
  q->o_entry_d = 2013;
  q->remote = 0;

  ol_cnt = q->ol_cnt;
  assert(ol_cnt <= TPCC_MAX_OL_PER_ORDER);
  for (int o = 0; o < ol_cnt; o++) {
    struct item *i = &q->item[o];

    do {
      i->ol_i_id = NURand(&p->seed, 8191, 1, TPCC_MAX_ITEMS);

      // no duplicates
      dup = 0;
      for (int j = 0; j < o; j++)
        if (q->item[j].ol_i_id == i->ol_i_id) {
          dup = 1;
          break;
        }
    } while(dup);

    int x = URand(&p->seed, 1, 100);
    if (g_dist_threshold == 100)
      x = 2;

    if (x > 1 || g_nservers == 1) {
    //if (1) {
      i->ol_supply_w_id = (s) + 1;;
    } else {
      while ((i->ol_supply_w_id = URand(&p->seed, 1, g_nservers)) == q->w_id)
        ;

      q->remote = 1;
    }

    assert(i->ol_supply_w_id != 0);

    i->ol_quantity = URand(&p->seed, 1, 10);
  }
}

void tpcc_get_next_orderstatus_query(struct hash_table *hash_table, int s,
    void *arg)
{

  struct partition *p = &hash_table->partitions[s];
  struct tpcc_query *q = (struct tpcc_query *) arg;
  q->w_id = (s) + 1;
  q->d_id = URand(&p->seed, 1, TPCC_NDIST_PER_WH);

  int y = URand(&p->seed, 1, 100);
  if (y <= 60) {
    // by last name
    q->by_last_name = TRUE;
    set_last_name(NURand(&p->seed, 255, 0, 999), q->c_last);
  } else {
    // by cust id
    q->by_last_name = FALSE;
    q->c_id = NURand(&p->seed, 1023, 1, TPCC_NCUST_PER_DIST);
  }
  q->c_w_id = (s) + 1;
}

void tpcc_get_next_delivery_query(struct hash_table *hash_table, int s,
    void *arg)
{
  struct partition *p = &hash_table->partitions[s];
  struct tpcc_query *q = (struct tpcc_query *) arg;
  q->w_id = (s) + 1;
  q->d_id = URand(&p->seed, 1, TPCC_NDIST_PER_WH);
  q->o_carrier_id = URand(&p->seed, 1, 10);

}

void tpcc_get_next_stocklevel_query(struct hash_table *hash_table, int s,
    void *arg)
{
  struct partition *p = &hash_table->partitions[s];
  struct tpcc_query *q = (struct tpcc_query *) arg;
  q->w_id = (s) + 1;
  q->d_id = URand(&p->seed, 1, TPCC_NDIST_PER_WH);
  q->threshold = URand(&p->seed, 10, 20);

}


static uint64_t cust_derive_key(char * c_last, int c_d_id, int c_w_id)
{
  uint64_t key = 0;
  char offset = 'A';
  for (uint32_t i = 0; i < strlen(c_last); i++)
    key = (key << 1) + (c_last[i] - offset);
  key = key << 10;
  key += c_w_id * TPCC_NDIST_PER_WH + c_d_id;

  return key;
}

static void load_stock(int w_id, struct partition *p)
{
  struct elem *e;
  hash_key key, base_sid, sid;
  struct tpcc_stock *r;

  base_sid = w_id * TPCC_MAX_ITEMS;
  for (int i = 1; i <= TPCC_MAX_ITEMS; i++) {
    sid = base_sid + i;
    key = MAKE_HASH_KEY(STOCK_TID, sid);
    e = hash_insert(p, key, sizeof(struct tpcc_stock), NULL);
    assert(e);

    r = (struct tpcc_stock *) e->value;
    r->s_i_id = i;
    r->s_w_id = w_id;
    r->s_quantity = URand(&p->seed, 10, 100);
    for (int j = 0; j < 10; j++)
      make_alpha_string(&p->seed, 24, 24, r->s_dist[j]);

    r->s_ytd = 0;
    r->s_order_cnt = 0;
    r->s_remote_cnt = 0;

    int len = make_alpha_string(&p->seed, 26, 50, r->s_data);
    if (RAND(&p->seed, 100) < 10) {
      int idx = URand(&p->seed, 0, len - 8);
      memcpy(&r->s_data[idx], "original", 8);
    }

    p->ninserts++;
    e->ref_count++;
  }
}

static void load_history(int w_id, struct partition *p)
{
  struct elem *e;
  struct tpcc_history *r;
  hash_key key, pkey;

  for (int d = 1; d <= TPCC_NDIST_PER_WH; d++) {
    for (int c = 1; c <= TPCC_NCUST_PER_DIST; c++) {
      pkey = MAKE_CUST_KEY(w_id, d, c);
      key = MAKE_HASH_KEY(HISTORY_TID, pkey);

      e = hash_insert(p, key, sizeof(struct tpcc_history), NULL);
      assert(e);

      r = (struct tpcc_history *) e->value;
      r->h_c_id = c;
      r->h_c_d_id = d;
      r->h_c_w_id = w_id;
      r->h_d_id = d;
      r->h_w_id = w_id;
      r->h_date = 0;
      r->h_amount = 10.0;
      make_alpha_string(&p->seed, 12, 24, r->h_data);

      p->ninserts++;
      e->ref_count++;
    }
  }
}

static void load_customer(int w_id, struct partition *p)
{
  struct elem *e;
  struct tpcc_customer *r;
  hash_key key;

  for (int d = 1; d <= TPCC_NDIST_PER_WH; d++) {
    for (int c = 1; c <= TPCC_NCUST_PER_DIST; c++) {
      uint64_t ckey = MAKE_CUST_KEY(w_id, d, c);
      key = MAKE_HASH_KEY(CUSTOMER_TID, ckey);

      e = hash_insert(p, key, sizeof(struct tpcc_customer), NULL);
      assert(e);

      e->ref_count++;
      p->ninserts++;

      r = (struct tpcc_customer *) e->value;
      r->c_id = c;
      r->c_d_id = d;
      r->c_w_id = w_id;

      if (c <= 1000)
        set_last_name(c - 1, r->c_last);
      else
        set_last_name(NURand(&p->seed, 255,0,999), r->c_last);

      memcpy(r->c_middle, "OE", 2);

      make_alpha_string(&p->seed, FIRST_NAME_MIN_LEN, FIRST_NAME_LEN, r->c_first);

      make_alpha_string(&p->seed, 10, 20, r->c_street[0]);
      make_alpha_string(&p->seed, 10, 20, r->c_street[1]);
      make_alpha_string(&p->seed, 10, 20, r->c_city);
      make_alpha_string(&p->seed, 2, 2, r->c_state); /* State */
      make_numeric_string(&p->seed, 9, 9, r->c_zip); /* Zip */
      make_numeric_string(&p->seed, 16, 16, r->c_phone); /* Zip */
      r->c_since = 0;
      r->c_credit_lim = 50000;
      r->c_delivery_cnt = 0;
      make_alpha_string(&p->seed, 300, 500, r->c_data);

      if (RAND(&p->seed, 10) == 0) {
        r->c_credit[0] = 'G';
      } else {
        r->c_credit[0] = 'B';
      }
      r->c_credit[1] = 'C';
      r->c_discount = (double)RAND(&p->seed, 5000) / 10000;
      r->c_balance = -10.0;
      r->c_ytd_payment = 10.0;
      r->c_payment_cnt = 1;

      /* create secondary index using the main hash table itself.
       * we can do this by deriving a key from the last name,dist,wh id
       * and using it to create a new record which will contain both
       * the real key of all records with that last name
       * XXX: Note that this key is not unique - so all names hashing to
       * the same key will hash to the same key. Thus, ppl with different
       * last names might hash to the same sr record.
       */
      hash_key sr_dkey = cust_derive_key(r->c_last, d, w_id);
      hash_key sr_key = MAKE_HASH_KEY(CUSTOMER_SIDX_TID, sr_dkey);

      // pull up the record if its already there
      struct secondary_record *sr = NULL;
      struct elem *sie = hash_lookup(p, sr_key);
      int sr_idx, sr_nids;

      if (sie) {
        sr = (struct secondary_record *) sie->value;
        sr_idx = sr->sr_idx;
        sr_nids = sr->sr_nids;

      } else {
        sie = hash_insert(p, sr_key, sizeof(struct secondary_record), NULL);
        assert(sie);

        sr = (struct secondary_record *) sie->value;

        /* XXX: memory leak possibility - if this record is ever freed
         * this malloc wont be released
         */
        sr->sr_rids = malloc(sizeof(hash_key) * NDEFAULT_RIDS);
        assert(sr->sr_rids);

        sr->sr_idx = sr_idx = 0;
        sr->sr_nids = sr_nids = NDEFAULT_RIDS;

        sie->ref_count++;
        p->ninserts++;
      }

      assert(sr_idx < sr_nids);

      /* add this record to the index */
      sr->sr_rids[sr_idx] = key;
      if (++sr_idx == sr_nids) {
        // reallocate the record array
        sr_nids *= 2;
        sr->sr_rids = realloc(sr->sr_rids, sizeof(hash_key) * sr_nids);
        assert(sr->sr_rids);
      }

      sr->sr_idx = sr_idx;
      sr->sr_nids = sr_nids;
    }
  }
}

void init_permutation(struct partition *p, uint64_t *cperm)
{
    int i;

    for(i = 0; i < TPCC_NCUST_PER_DIST; i++) {
        cperm[i] = i+1;
    }

    // shuffle
    for(i = 0; i < TPCC_NCUST_PER_DIST - 1; i++) {
        uint64_t j = URand(&p->seed, i + 1, TPCC_NCUST_PER_DIST - 1);
        uint64_t tmp = cperm[i];
        cperm[i] = cperm[j];
        cperm[j] = tmp;
    }

    return;
}


static void load_order(int w_id, struct partition *p)
{
  struct elem *e;
  struct tpcc_order *r;
  hash_key key, ckey;

  uint64_t *cperm = malloc(sizeof(uint64_t) * TPCC_NCUST_PER_DIST);
  assert(cperm);

  for (int d = 1; d <= TPCC_NDIST_PER_WH; d++) {
    init_permutation(p, cperm);

    for (int o = 1; o <= TPCC_NCUST_PER_DIST; o++) {
      ckey = MAKE_CUST_KEY(w_id, d, o);
      key = MAKE_HASH_KEY(ORDER_TID, ckey);

      e = hash_insert(p, key, sizeof(struct tpcc_order), NULL);
      assert(e);

      p->ninserts++;
      e->ref_count++;

      r = (struct tpcc_order *) e->value;

      int c_id = cperm[o - 1];
      r->o_id = o;
      r->o_c_id = c_id;
      r->o_d_id = d;
      r->o_w_id = w_id;
      int o_entry = 2013;
      r->o_entry_d = 2013;
      if (o < 2101)
        r->o_carrier_id = URand(&p->seed, 1, 10);
      else
        r->o_carrier_id = 0;
      int o_ol_cnt = URand(&p->seed, 5, 15);
      r->o_ol_cnt = o_ol_cnt;
      r->o_all_local = 1;

      for (int ol = 1; ol <= o_ol_cnt; ol++) {
        hash_key ol_pkey = MAKE_OL_KEY(w_id, d, o, ol);
        hash_key ol_key = MAKE_HASH_KEY(ORDER_LINE_TID, ol_pkey);

        struct elem *e_ol = hash_insert(p, ol_key,
            sizeof(struct tpcc_order_line), NULL);
        assert(e_ol);

        p->ninserts++;
        e_ol->ref_count++;

        struct tpcc_order_line *r_ol = (struct tpcc_order_line *) e_ol->value;
        r_ol->ol_o_id = o;
        r_ol->ol_d_id = d;
        r_ol->ol_w_id = w_id;
        r_ol->ol_number = ol;
        r_ol->ol_i_id = URand(&p->seed, 1, 100000);
        r_ol->ol_supply_w_id = w_id;

        if (o < 2101) {
          r_ol->ol_delivery_d = o_entry;
          r_ol->ol_amount = 0;
        } else {
          r_ol->ol_delivery_d = 0;
          r_ol->ol_amount = (double)URand(&p->seed, 1, 999999)/100;
        }
        r_ol->ol_quantity = 5;
        make_alpha_string(&p->seed, 24, 24, r_ol->ol_dist_info);

        //          uint64_t key = orderlineKey(wid, did, oid);
        //          index_insert(i_orderline, key, row, wh_to_part(wid));

        //          key = distKey(did, wid);
        //          index_insert(i_orderline_wd, key, row, wh_to_part(wid));
      }

      // NEW ORDER
      if (o > 2100) {
        key = MAKE_HASH_KEY(NEW_ORDER_TID, ckey);
        struct elem *e_no = hash_insert(p, key,
            sizeof(struct tpcc_new_order), NULL);
        assert(e_no);

        p->ninserts++;
        e_no->ref_count++;

        struct tpcc_new_order *r_no = (struct tpcc_new_order *) e_no->value;

        r_no->no_o_id = o;
        r_no->no_d_id = d;
        r_no->no_w_id = w_id;
      }
    }
  }

  free(cperm);
}

void load_item(int w_id, struct partition *p)
{
  struct elem *e;
  struct tpcc_item *r;
  hash_key key;
  int data_len;

  assert(w_id == 1);

  for (int i = 1; i <= TPCC_MAX_ITEMS; i++) {
    key = MAKE_HASH_KEY(ITEM_TID, i);
    e = hash_insert(p, key, sizeof(struct tpcc_item), NULL);
    assert(e);

    p->ninserts++;
    e->ref_count++;

    r = (struct tpcc_item *) e->value;
    r->i_id = i;
    r->i_im_id = URand(&p->seed, 1L,10000L);
    make_alpha_string(&p->seed, 14, 24, r->i_name);
    r->i_price = URand(&p->seed, 1, 100);
    data_len = make_alpha_string(&p->seed, 26, 50, r->i_data);

    // TODO in TPCC, "original" should start at a random position
    if (RAND(&p->seed, 10) == 0) {
      int idx = URand(&p->seed, 0, data_len - 8);
      memcpy(&r->i_data[idx], "original", 8);
    }
  }
}

void load_warehouse(int w_id, struct partition *p)
{
  struct elem *e;
  struct tpcc_warehouse *r;
  hash_key key;

  key = MAKE_HASH_KEY(WAREHOUSE_TID, w_id);
  e = hash_insert(p, key, sizeof(struct tpcc_warehouse), NULL);
  assert(e);

  p->ninserts++;
  e->ref_count++;

  r = (struct tpcc_warehouse *) e->value;
  r->w_id = w_id;

  make_alpha_string(&p->seed, 6, 10, r->w_name);
  make_alpha_string(&p->seed, 10, 20, r->w_street[0]);
  make_alpha_string(&p->seed, 10, 20, r->w_street[1]);
  make_alpha_string(&p->seed, 10, 20, r->w_city);
  make_alpha_string(&p->seed, 2, 2, r->w_state);
  make_alpha_string(&p->seed, 9, 9, r->w_zip);
  double tax = (double)URand(&p->seed, 0L,200L)/1000.0;
  double w_ytd=300000.00;
  r->w_tax = tax;
  r->w_ytd = w_ytd;
}

void load_district(int w_id, struct partition *p)
{
  struct elem *e;
  struct tpcc_district *r;
  hash_key key, dkey;

  for (int d = 1; d <= TPCC_NDIST_PER_WH; d++) {
    dkey = MAKE_DIST_KEY(w_id, d);
    key = MAKE_HASH_KEY(DISTRICT_TID, dkey);
    e = hash_insert(p, key, sizeof(struct tpcc_district), NULL);
    assert(e);

    p->ninserts++;
    e->ref_count++;

    r = (struct tpcc_district *) e->value;
    r->d_id = d;
    r->d_w_id = w_id;

    make_alpha_string(&p->seed, 6, 10, r->d_name);
    make_alpha_string(&p->seed, 10, 20, r->d_street[0]);
    make_alpha_string(&p->seed, 10, 20, r->d_street[1]);
    make_alpha_string(&p->seed, 10, 20, r->d_city);
    make_alpha_string(&p->seed, 2, 2, r->d_state);
    make_alpha_string(&p->seed, 9, 9, r->d_zip);
    double tax = (double)URand(&p->seed, 0L,200L)/1000.0;
    double w_ytd=30000.00;
    r->d_tax = tax;
    r->d_ytd = w_ytd;
    r->d_next_o_id = 3001;
  }
}

void tpcc_load_warehouse(struct hash_table *hash_table, int w_id, int id)
{
  struct partition *p = &hash_table->partitions[id];
  struct partition *item = hash_table->g_partition;

  printf("srv (%d): Loading stock..\n", id);
  load_stock(w_id, p);

  printf("srv (%d): Loading history..\n", id);
  load_history(w_id, p);

  printf("srv (%d): Loading customer..\n", id);
  load_customer(w_id, p);

  printf("srv (%d): Loading order..\n", id);
  load_order(w_id, p);
  if (w_id == 1) {
    load_item(w_id, item);
    printf("srv (%d): Loading item..\n", id);
  }

  printf("srv (%d): Loading wh..\n", id);
  load_warehouse(w_id, p);

  printf("srv (%d): Loading district..\n", id);
  load_district(w_id, p);
}

void tpcc_load_data(struct hash_table *hash_table, int id)
{
  tpcc_load_warehouse(hash_table, id + 1, id);
}

int tpcc_run_neworder_txn(struct hash_table *hash_table, int id,
    struct tpcc_query *q, struct task *ctask)
{
  uint64_t key, pkey;
  struct hash_op op;
  struct partition *item_p = hash_table->g_partition;
  struct txn_ctx *ctx = &ctask->txn_ctx;
  char remote = q->remote;
  int w_id = q->w_id;
  int d_id = q->d_id;
  int c_id = q->c_id;
  int ol_cnt = q->ol_cnt;
  int o_entry_d = q->o_entry_d;
  int r = TXN_COMMIT;

  //assert(q->w_id == id + 1);

#if SHARED_NOTHING
  /* we have to acquire all partition locks in warehouse order.  */
  char bits[BITNSLOTS(MAX_SERVERS)];
  int i, alock_state, partitions[MAX_SERVERS], npartitions;

  memset(bits, 0, sizeof(bits));
  BITSET(bits, w_id);

  for (i = 0; i < ol_cnt; i++) {
    BITSET(bits, q->item[i].ol_supply_w_id);
  }

  assert(BITTEST(bits, 0) == 0);

  npartitions = 0;
  for (i = 1; i <= g_nservers; i++) {
    if (BITTEST(bits, i)) {
      partitions[npartitions++] = i;
      LATCH_ACQUIRE(&hash_table->partitions[i - 1].latch, &alock_state);
    }
  }

#endif

  txn_start(hash_table, id, ctx);

  /*
   * EXEC SQL SELECT c_discount, c_last, c_credit, w_tax
   * INTO :c_discount, :c_last, :c_credit, :w_tax
   * FROM customer, warehouse
   * WHERE w_id = :w_id AND c_w_id = w_id AND c_d_id = :d_id AND c_id = :c_id;
   */
  pkey = MAKE_HASH_KEY(WAREHOUSE_TID, w_id);
  MAKE_OP(op, OPTYPE_LOOKUP, 0, pkey);

  struct tpcc_warehouse *w_r =
    (struct tpcc_warehouse *) txn_op(ctask, hash_table, id, &op, w_id - 1);

  if (!w_r) {
    dprint("srv(%d): Aborting due to key %"PRId64"\n", id, pkey);
    r = TXN_ABORT;
    goto final;
  }

  double w_tax = w_r->w_tax;
  key = MAKE_CUST_KEY(w_id, d_id, c_id);
  pkey = MAKE_HASH_KEY(CUSTOMER_TID, key);
  MAKE_OP(op, OPTYPE_LOOKUP, 0, pkey);

  struct tpcc_customer *c_r =
    (struct tpcc_customer *) txn_op(ctask, hash_table, id, &op, w_id - 1);
  if (!c_r) {
    dprint("srv(%d): Aborting due to key %"PRId64"\n", id, pkey);
    r = TXN_ABORT;
    goto final;
  }

  uint64_t c_discount = c_r->c_discount;

  /*
   * EXEC SQL SELECT d_next_o_id, d_tax
   *  INTO :d_next_o_id, :d_tax
   *  FROM district WHERE d_id = :d_id AND d_w_id = :w_id;
   */
  key = MAKE_DIST_KEY(w_id, d_id);
  pkey = MAKE_HASH_KEY(DISTRICT_TID, key);
  MAKE_OP(op, OPTYPE_UPDATE, 0, pkey);
  struct tpcc_district *d_r =
    (struct tpcc_district *) txn_op(ctask, hash_table, id, &op, w_id - 1);
  if (!d_r) {
    dprint("srv(%d): Aborting due to key %"PRId64"\n", id, pkey);
    r = TXN_ABORT;
    goto final;
  }

  uint64_t o_id = d_r->d_next_o_id;
  double d_tax = d_r->d_tax;

  /*
   * EXEC SQL UPDATE district SET d _next_o_id = :d _next_o_id + 1
   * WHERE d _id = :d_id AN D d _w _id = :w _id ;
   */
  d_r->d_next_o_id++;

  /*
   * EXEC SQL INSERT IN TO ORDERS (o_id , o_d _id , o_w _id , o_c_id ,
   * o_entry_d , o_ol_cnt, o_all_local)
   * VALUES (:o_id , :d _id , :w _id , :c_id ,
   * :d atetime, :o_ol_cnt, :o_all_local);
   */
  key = MAKE_CUST_KEY(w_id, d_id, o_id);
  pkey = MAKE_HASH_KEY(ORDER_TID, key);
  MAKE_OP(op, OPTYPE_INSERT, (sizeof(struct tpcc_order)), pkey);
  struct tpcc_order *o_r =
    (struct tpcc_order *) txn_op(ctask, hash_table, id, &op, w_id - 1);
  assert(o_r);

  //dprint("srv(%d): inserted %"PRId64"\n", id, pkey);
  //printf("srv(%d): inserted w %d d %d oid %d key %"PRIu64"\n", id, w_id, d_id, o_id, key);

  o_r->o_id = o_id;
  o_r->o_c_id = c_id;
  o_r->o_d_id = d_id;
  o_r->o_w_id = w_id;
  o_r->o_entry_d = o_entry_d;
  o_r->o_ol_cnt = ol_cnt;

  // for now only local
  o_r->o_all_local = !remote;

  /*
   * EXEC SQL INSERT IN TO NEW_ORDER (no_o_id , no_d_id , no_w _id )
   * VALUES (:o_id , :d _id , :w _id );
   */
  pkey = MAKE_HASH_KEY(NEW_ORDER_TID, key);
  MAKE_OP(op, OPTYPE_INSERT, (sizeof(struct tpcc_new_order)), pkey);
  struct tpcc_new_order *no_r =
    (struct tpcc_new_order *) txn_op(ctask, hash_table, id, &op, w_id - 1);
  if(!no_r){
    printf("no_r w_id = %d d_id = %d o_id = %d\n",w_id,d_id,o_id);
    printf("srv(%d): inserted %"PRId64"\n", id, pkey);
  }
  assert(no_r);

  dprint("srv(%d): inserted %"PRId64"\n", id, pkey);

  no_r->no_o_id = o_id;
  no_r->no_d_id = d_id;
  no_r->no_w_id = w_id;

  for (int ol_number = 0; ol_number < ol_cnt; ol_number++) {
    uint64_t ol_i_id = q->item[ol_number].ol_i_id;
    uint64_t ol_supply_w_id = q->item[ol_number].ol_supply_w_id;
    uint64_t ol_quantity = q->item[ol_number].ol_quantity;

    /*
     * EXEC SQL SELECT i_price, i_name , i_data
     * INTO :i_price, :i_name, :i_data
     * FROM item WHERE i_id = ol_i_id
     */
    pkey = MAKE_HASH_KEY(ITEM_TID, ol_i_id);
    MAKE_OP(op, OPTYPE_LOOKUP, 0, pkey);
    struct tpcc_item *i_r =
      (struct tpcc_item *) txn_op(ctask, hash_table, id, &op, id);
    assert(i_r);
    if (!i_r) {
      dprint("srv(%d): Aborting due to key %"PRId64"\n", id, pkey);
      r = TXN_ABORT;
      goto final;
    }

    uint64_t i_price = i_r->i_price;
    //char *i_name = i_r->i_name;
    //char *i_data = i_r->i_data;

    /*
     * EXEC SQL SELECT s_quantity, s_d ata,
     * s_d ist_01, s_dist_02, s_d ist_03, s_d ist_04, s_d ist_05
     * s_d ist_06, s_dist_07, s_d ist_08, s_d ist_09, s_d ist_10
     * IN TO :s_quantity, :s_d ata,
     * :s_d ist_01, :s_d ist_02, :s_dist_03, :s_d ist_04, :s_d ist_05
     * :s_d ist_06, :s_d ist_07, :s_dist_08, :s_d ist_09, :s_d ist_10
     * FROM stock
     * WH ERE s_i_id = :ol_i_id AN D s_w _id = :ol_supply_w _id ;
     */
    key = MAKE_STOCK_KEY(ol_supply_w_id, ol_i_id);
    pkey = MAKE_HASH_KEY(STOCK_TID, key);
    MAKE_OP(op, OPTYPE_UPDATE, 0, pkey);

    struct tpcc_stock *s_r = NULL;
    s_r = (struct tpcc_stock *) txn_op(ctask, hash_table, id, &op, ol_supply_w_id - 1);
    if (!s_r) {
      dprint("srv(%d): Aborting due to key %"PRId64"\n", id, pkey);
      r = TXN_ABORT;
      goto final;
    }
    uint64_t s_quantity = s_r->s_quantity;
    /*
    char *s_data = s_r->s_data;
    char *s_dist[10];
    for (int i = 0; i < 10; i++)
      s_dist[i] = s_r->s_dist[i];
    */

    s_r->s_ytd += ol_quantity;
    s_r->s_order_cnt++;
    if (remote) {
      s_r->s_remote_cnt++;
    }

    uint64_t quantity;
    if (s_quantity > ol_quantity + 10)
      quantity = s_quantity - ol_quantity;
    else
      quantity = s_quantity - ol_quantity + 91;

    s_r->s_quantity = quantity;

    /*
     * EXEC SQL INSERT
     * INTO order_line(ol_o_id, ol_d_id, ol_w_id, ol_number,
     * ol_i_id, ol_supply_w_id,
     * ol_quantity, ol_amount, ol_dist_info)
     * VALUES(:o_id, :d_id, :w_id, :ol_number,
     * :ol_i_id, :ol_supply_w_id,
     * :ol_quantity, :ol_amount, :ol_dist_info);
     */
    key = MAKE_OL_KEY(w_id, d_id, o_id, ol_number);
    pkey = MAKE_HASH_KEY(ORDER_LINE_TID, key);
    MAKE_OP(op, OPTYPE_INSERT, (sizeof(struct tpcc_order_line)), pkey);
    struct tpcc_order_line *ol_r =
      (struct tpcc_order_line *) txn_op(ctask, hash_table, id, &op, w_id - 1);
      if(!ol_r){
        printf("no_r w_id = %d d_id = %d o_id = %d\n",w_id,d_id,o_id);
      }
    assert(ol_r);
    dprint("srv(%d): inserted %"PRId64"\n", id, pkey);

    double ol_amount = ol_quantity * i_price * (1 + w_tax + d_tax) * (1 - c_discount);

    ol_r->ol_o_id = o_id;
    ol_r->ol_d_id = d_id;
    ol_r->ol_w_id = w_id;
    ol_r->ol_number = ol_number;
    ol_r->ol_i_id = ol_i_id;
    ol_r->ol_supply_w_id = ol_supply_w_id;
    ol_r->ol_quantity = ol_quantity;
    ol_r->ol_amount = ol_amount;

  }

final:

#if SHARED_NOTHING
  /* in shared nothing case, there is now way a transaction will ever be
   * aborted. We always get all partition locks before we start. So txn
   * execution is completely serial. Likewise, there is no need to reset
   * reference counts in records for logical locks as we don't need them.
   *
   * So just unlatch the partitions and be done
   */

  assert (r == TXN_COMMIT);
  for (i = 0; i < npartitions; i++) {
    int t = partitions[i];
    assert (BITTEST(bits, t));
    LATCH_RELEASE(&hash_table->partitions[t - 1].latch, &alock_state);
  }
#else
  if (r == TXN_COMMIT)
    r = txn_commit(ctask, hash_table, id, TXN_SINGLE);
  else
    txn_abort(ctask, hash_table, id, TXN_SINGLE);
#endif

  return r;
}

int tpcc_run_payment_txn (struct hash_table *hash_table, int id,
    struct tpcc_query *q, struct task *ctask)
{
  struct hash_op op;
  uint64_t key, pkey;
  int w_id = q->w_id;
  int d_id = q->d_id;
  int c_w_id = q->c_w_id;
  int c_d_id = q->c_d_id;
  int c_id = q->c_id;
  double h_amount = q->h_amount;
  struct txn_ctx *ctx = &ctask->txn_ctx;
  short opids[MAX_OPS_PER_QUERY];
  short nopids = 0;

  int r = TXN_COMMIT;

  //assert(q->w_id == id + 1);

#if SHARED_NOTHING
  /* we have to acquire all partition locks in warehouse order.  */
  char bits[BITNSLOTS(MAX_SERVERS)];
  int i, alock_state, partitions[MAX_SERVERS], npartitions;

  memset(bits, 0, sizeof(bits));
  BITSET(bits, w_id);
  BITSET(bits, c_w_id);

  assert(BITTEST(bits, 0) == 0);

  npartitions = 0;
  for (i = 1; i <= g_nservers; i++) {
    if (BITTEST(bits, i)) {
      partitions[npartitions++] = i;
      LATCH_ACQUIRE(&hash_table->partitions[i - 1].latch, &alock_state);
    }
  }

#endif

  txn_start(hash_table, id, ctx);

  /*====================================================+
      EXEC SQL UPDATE warehouse SET w_ytd = w_ytd + :h_amount
      WHERE w_id=:w_id;
  +====================================================*/
  /*===================================================================+
      EXEC SQL SELECT w_street_1, w_street_2, w_city, w_state, w_zip, w_name
      INTO :w_street_1, :w_street_2, :w_city, :w_state, :w_zip, :w_name
      FROM warehouse
      WHERE w_id=:w_id;
  +===================================================================*/

  pkey = MAKE_HASH_KEY(WAREHOUSE_TID, w_id);
  MAKE_OP(op, OPTYPE_UPDATE, 0, pkey);
  opids[nopids++] = 0;
  struct tpcc_warehouse *w_r =
    (struct tpcc_warehouse *) txn_op(ctask, hash_table, id, &op, w_id - 1);
  CHK_ABORT(w_r);

  w_r->w_ytd += h_amount;

  /*=====================================================+
      EXEC SQL UPDATE district SET d_ytd = d_ytd + :h_amount
      WHERE d_w_id=:w_id AND d_id=:d_id;
  =====================================================*/
  /*====================================================================+
      EXEC SQL SELECT d_street_1, d_street_2, d_city, d_state, d_zip, d_name
      INTO :d_street_1, :d_street_2, :d_city, :d_state, :d_zip, :d_name
      FROM district
      WHERE d_w_id=:w_id AND d_id=:d_id;
  +====================================================================*/
  key = MAKE_DIST_KEY(w_id, d_id);
  pkey = MAKE_HASH_KEY(DISTRICT_TID, key);
  MAKE_OP(op, OPTYPE_UPDATE, 0, pkey);
  opids[nopids++] = 0;
  struct tpcc_district *d_r =
    (struct tpcc_district *) txn_op(ctask, hash_table, id, &op, w_id - 1);
  CHK_ABORT(d_r);

  d_r->d_ytd += h_amount;

  struct tpcc_customer *c_r;
  if (q->by_last_name) {
    /*==========================================================+
      EXEC SQL SELECT count(c_id) INTO :namecnt
      FROM customer
      WHERE c_last=:c_last AND c_d_id=:c_d_id AND c_w_id=:c_w_id;
      +==========================================================*/
    key = cust_derive_key(q->c_last, c_d_id, c_w_id);
    pkey = MAKE_HASH_KEY(CUSTOMER_SIDX_TID, key);
    MAKE_OP(op, OPTYPE_LOOKUP, 0, pkey);
    struct secondary_record *sr = NULL;

    sr =  (struct secondary_record *) txn_op(ctask, hash_table, id, &op, c_w_id - 1);
    opids[nopids++] = 0;

    CHK_ABORT(sr);

    struct tpcc_customer *c_recs[MAX_OPS_PER_QUERY];
    assert(sr->sr_idx < MAX_OPS_PER_QUERY);

    int nmatch = 0;
#if defined(SHARED_EVERYTHING) || defined(SHARED_NOTHING)
    nmatch = fetch_cust_records(hash_table, id, ctask, c_recs, sr, q,
        opids, &nopids);
#else
    if (c_w_id == w_id) {
      nmatch = fetch_cust_records(hash_table, id, ctask, c_recs, sr, q,
          opids, &nopids);
    } else {
      nmatch = batch_fetch_cust_records(hash_table, id, ctask, c_recs, sr, q,
          opids, &nopids);
    }
#endif

    if (nmatch == -1) {
      r = TXN_ABORT;
      goto final;
    }

    assert(nmatch > 0);

    /*============================================================================+
        for (n=0; n<namecnt/2; n++) {
            EXEC SQL FETCH c_byname
            INTO :c_first, :c_middle, :c_id,
                 :c_street_1, :c_street_2, :c_city, :c_state, :c_zip,
                 :c_phone, :c_credit, :c_credit_lim, :c_discount, :c_balance, :c_since;
            }
        EXEC SQL CLOSE c_byname;
    +=============================================================================*/
    // now sort based on first name and get middle element
    // XXX: Inefficient bubble sort for now. Also the strcmp below has a huge
    // overhead. We need some sorted secondary index structure
    for (int i = 0; i < nmatch; i++) {
      for (int j = i + 1; j < nmatch; j++) {
        if (strcmp(c_recs[i]->c_first, c_recs[j]->c_first) > 0) {
          struct tpcc_customer *tmp = c_recs[i];
          c_recs[i] = c_recs[j];
          c_recs[j] = tmp;
        }
      }
    }

    c_r = c_recs[nmatch / 2];

  } else { // search customers by cust_id
    /*=====================================================================+
        EXEC SQL SELECT c_first, c_middle, c_last, c_street_1, c_street_2,
            c_city, c_state, c_zip, c_phone, c_credit, c_credit_lim,
            c_discount, c_balance, c_since
          INTO :c_first, :c_middle, :c_last, :c_street_1, :c_street_2,
            :c_city, :c_state, :c_zip, :c_phone, :c_credit, :c_credit_lim,
            :c_discount, :c_balance, :c_since
          FROM customer
          WHERE c_w_id=:c_w_id AND c_d_id=:c_d_id AND c_id=:c_id;
      +======================================================================*/
    key = MAKE_CUST_KEY(c_w_id, c_d_id, c_id);
    pkey = MAKE_HASH_KEY(CUSTOMER_TID, key);
    MAKE_OP(op, OPTYPE_LOOKUP, 0, pkey);

    c_r = (struct tpcc_customer *) txn_op(ctask, hash_table, id, &op, c_w_id - 1);
    opids[nopids++] = 0;

    CHK_ABORT(c_r);
  }

  /*======================================================================+
    EXEC SQL UPDATE customer SET c_balance = :c_balance, c_data = :c_new_data
    WHERE c_w_id = :c_w_id AND c_d_id = :c_d_id AND c_id = :c_id;
    +======================================================================*/
  c_r->c_balance -= h_amount;
  c_r->c_ytd_payment += h_amount;
  c_r->c_payment_cnt++;

  if (strstr(c_r->c_credit, "BC") ) {

    /*=====================================================+
      EXEC SQL SELECT c_data
            INTO :c_data
            FROM customer
            WHERE c_w_id=:c_w_id AND c_d_id=:c_d_id AND c_id=:c_id;
            +=====================================================*/
    char c_new_data[501];
    sprintf(c_new_data,"| %4d %2d %4d %2d %4d $%7.2f",
        c_id, c_d_id, c_w_id, d_id, w_id, h_amount);
    strncat(c_new_data, c_r->c_data, 500 - strlen(c_new_data));
    strcpy(c_r->c_data, c_new_data);
  }
/*
    char h_data[25];
    char * w_name = r_wh_local->get_value("W_NAME");
    char * d_name = r_dist_local->get_value("D_NAME");
    strncpy(h_data, w_name, 10);
    int length = strlen(h_data);
    if (length > 10) length = 10;
    strcpy(&h_data[length], "    ");
    strncpy(&h_data[length + 4], d_name, 10);
    h_data[length+14] = '\0';
*/
    /*=============================================================================+
      EXEC SQL INSERT INTO
      history (h_c_d_id, h_c_w_id, h_c_id, h_d_id, h_w_id, h_date, h_amount, h_data)
      VALUES (:c_d_id, :c_w_id, :c_id, :d_id, :w_id, :datetime, :h_amount, :h_data);
      +=============================================================================*/
  pkey = MAKE_CUST_KEY(w_id, d_id, c_id);
  key = MAKE_HASH_KEY(HISTORY_TID, pkey);
  MAKE_OP(op, OPTYPE_INSERT, (sizeof(struct tpcc_order)), key);
  opids[nopids++] = 0;
  struct tpcc_history *h_r =
    (struct tpcc_history *) txn_op(ctask, hash_table, id, &op, w_id - 1);
  CHK_ABORT(h_r);

  h_r->h_c_id = c_id;
  h_r->h_c_d_id = c_d_id;
  h_r->h_c_w_id = c_w_id;
  h_r->h_d_id = d_id;
  h_r->h_w_id = w_id;
  h_r->h_date = 2013; /* XXX: Why 2013? */
  h_r->h_amount = h_amount;

final:

#if SHARED_NOTHING
  /* in shared nothing case, there is now way a transaction will ever be
   * aborted. We always get all partition locks before we start. So txn
   * execution is completely serial. Likewise, there is no need to reset
   * reference counts in records for logical locks as we don't need them.
   *
   * So just unlatch the partitions and be done
   */

  assert (r == TXN_COMMIT);
  for (i = 0; i < npartitions; i++) {
    int t = partitions[i];
    assert (BITTEST(bits, t));
    LATCH_RELEASE(&hash_table->partitions[t - 1].latch, &alock_state);
  }

#else
  txn_finish(ctask, hash_table, id, r, TXN_SINGLE, opids);
#endif

  return r;
}
int tpcc_run_orderstatus_txn(struct hash_table *hash_table, int id,
    struct tpcc_query *q, struct task *ctask)
 {
	/*
	This transaction accesses
	customer
	orders
	order_line
	*/
  struct hash_op op;
  uint64_t key, pkey;
  int ol_number = 1;
  int w_id = q->w_id;
  int d_id = q->d_id;
  int c_id = q->c_id;
  int c_w_id = q->c_w_id;
  short opids[MAX_OPS_PER_QUERY];
  short nopids = 0;

  int r = TXN_COMMIT;

  //assert(q->w_id == id + 1);

  #if SHARED_NOTHING
    /* we have to acquire all partition locks in warehouse order.  */
    char bits[BITNSLOTS(MAX_SERVERS)];
    int i, alock_state, partitions[MAX_SERVERS], npartitions;

    memset(bits, 0, sizeof(bits));

    //we should aquire the lock for our warehouse
    BITSET(bits, w_id);

    assert(BITTEST(bits, 0) == 0);
    //we have to save the number of partitions because we need it when we want to free the partitions
    npartitions = 0;
    //go on the servers one by one and lock the partitons for which we have a 1 bit in the bits array
    for (i = 1; i <= g_nservers; i++) {
      if (BITTEST(bits, i)) {
        partitions[npartitions++] = i;
        LATCH_ACQUIRE(&hash_table->partitions[i - 1].latch, &alock_state);
      }
    }

  #endif


  struct tpcc_customer *c_r;
  if (q->by_last_name) {
  /*
  * EXEC SQL SELECT count(c_id) INTO :namecnt
  * FROM customer
  * WHERE c_last=:c_last AND c_d_id=:d_id AND c_w_id=:w_id;
  */
    key = cust_derive_key(q->c_last, d_id, w_id);
    pkey = MAKE_HASH_KEY(CUSTOMER_SIDX_TID, key);
    MAKE_OP(op, OPTYPE_LOOKUP, 0, pkey);
    assert(op.optype == 0x00000000);
    struct secondary_record *sr = NULL;

    sr =  (struct secondary_record *) txn_op(ctask, hash_table, id, &op, w_id - 1);
    opids[nopids++] = 0;

    CHK_ABORT(sr);

    struct tpcc_customer *c_recs[MAX_OPS_PER_QUERY];
    assert(sr->sr_idx < MAX_OPS_PER_QUERY);

    int nmatch = 0;
    nmatch = fetch_cust_records(hash_table, id, ctask, c_recs, sr, q,
      opids, &nopids);

    if (nmatch == -1) {
      r = TXN_ABORT;
      goto final;
    }

    assert(nmatch > 0);

  /*
   * for (n=0; n<namecnt/2; n++)
   * {
   * EXEC SQL FETCH c_name
   * INTO :c_balance, :c_first, :c_middle, :c_id;
   * }
   */
   // now sort based on first name and get middle element
   // XXX: Inefficient bubble sort for now. Also the strcmp below has a huge
   // overhead. We need some sorted secondary index structure
   for (int i = 0; i < nmatch; i++) {
     for (int j = i + 1; j < nmatch; j++) {
       if (strcmp(c_recs[i]->c_first, c_recs[j]->c_first) > 0) {
         struct tpcc_customer *tmp = c_recs[i];
         c_recs[i] = c_recs[j];
         c_recs[j] = tmp;
       }
     }
   }

   c_r = c_recs[nmatch / 2];
}
else{

  /*
   * EXEC SQL SELECT c_balance, c_first, c_middle, c_last
   * INTO :c_balance, :c_first, :c_middle, :c_last
   * FROM customer
   *  WHERE c_id=:c_id AND c_d_id=:d_id AND c_w_id=:w_id;
   */

   key = MAKE_CUST_KEY(w_id, d_id, c_id);
  pkey = MAKE_HASH_KEY(CUSTOMER_TID, key);
  MAKE_OP(op, OPTYPE_LOOKUP, 0, pkey);
  assert(op.optype == 0x00000000);

  struct tpcc_customer *c_r =
    (struct tpcc_customer *) txn_op(ctask, hash_table, id, &op, w_id - 1);

  CHK_ABORT(c_r);



  double c_balance = c_r->c_balance;
  char c_first[FIRST_NAME_LEN];
  for(int i = 0; i < FIRST_NAME_LEN; ++i){
    c_first[i] = c_r->c_first[i];
  }
  char c_middle[2];
  c_middle[0] = c_r->c_middle[0];
  c_middle[1] = c_r->c_middle[1];

	char c_last[LAST_NAME_LEN];
  for(int i = 0 ; i < LAST_NAME_LEN ; ++i){
    c_last[i] = c_r->c_last[i];
  }

  /*
   * EXEC SQL SELECT o_id, o_carrier_id, o_entry_d
   * INTO :o_id, :o_carrier_id, :entdate
   * FROM orders
   *  ORDER BY o_id DESC;
   */

   int counter1 = 0;
   int64_t o_id[TPCC_NCUST_PER_DIST];
   int64_t o_carrier_id[TPCC_NCUST_PER_DIST];
   int64_t entdate[TPCC_NCUST_PER_DIST];
   for (int o = TPCC_NCUST_PER_DIST ; o > 0 ; o --){
     key = MAKE_CUST_KEY(w_id, d_id, o);
     pkey = MAKE_HASH_KEY(ORDER_TID, key);
     MAKE_OP(op, OPTYPE_LOOKUP, 0, pkey);
     assert(op.optype == 0x00000000);
    }


  /*
   * EXEC SQL DECLARE c_line CURSOR FOR
   * SELECT ol_i_id, ol_supply_w_id, ol_quantity,
       ol_amount, ol_delivery_d
   * FROM order_line
   *  WHERE ol_o_id=:o_id AND ol_d_id=:d_id AND ol_w_id=:w_id;
   */

   int counter2 = 0;
 	 int64_t ol_i_id[TPCC_MAX_OL_PER_ORDER-5];
 	 int64_t ol_supply_w_id[TPCC_MAX_OL_PER_ORDER-5];
 	 int64_t ol_delivery_d[TPCC_MAX_OL_PER_ORDER-5];
 	 int64_t ol_quantity[TPCC_MAX_OL_PER_ORDER-5];
 	 double ol_amount[TPCC_MAX_OL_PER_ORDER-5];
   for(long ol_number = 5 ; ol_number < TPCC_MAX_OL_PER_ORDER ; ++ol_number){

      key = MAKE_OL_KEY(w_id, d_id, o_id[counter1], ol_number);
      pkey = MAKE_HASH_KEY(ORDER_LINE_TID, key);
      MAKE_OP(op, OPTYPE_LOOKUP, 0, pkey);
      assert(op.optype == 0x00000000);
      struct tpcc_order_line *ol_r =
        (struct tpcc_order_line *) txn_op(ctask, hash_table, id, &op, w_id - 1);
        if(ol_r){
          ol_i_id[counter2] = ol_r->ol_i_id;
          ol_supply_w_id[counter2] = ol_r->ol_supply_w_id;
          ol_quantity[counter2] = ol_r->ol_quantity;
          ol_amount[counter2] = ol_r->ol_amount;
          ol_delivery_d[counter2] = ol_r->ol_delivery_d;
          ++counter2;
        }
   }
}

final:
#if SHARED_NOTHING

  assert (r == TXN_COMMIT);
  for (i = 0; i < npartitions; i++) {
    int t = partitions[i];
    assert (BITTEST(bits, t));
    LATCH_RELEASE(&hash_table->partitions[t - 1].latch, &alock_state);
  }

#else
if (r == TXN_COMMIT)
  r = txn_commit(ctask, hash_table, id, TXN_SINGLE);
else
  txn_abort(ctask, hash_table, id, TXN_SINGLE);
#endif

  return r;

 }

 int tpcc_run_delivery_txn(struct hash_table *hash_table, int id,
      struct tpcc_query *q, struct task *ctask)
  {

	  	/*
 	This transaction accesses
 	new_order
 	orders
 	order_line
 	customer
 	*/
   struct hash_op op;
   uint64_t key, pkey;
   int w_id = q->w_id;
   int d_id = q->d_id;
   struct txn_ctx *ctx = &ctask->txn_ctx;
   short opids[MAX_OPS_PER_QUERY];

   int r = TXN_COMMIT;

   //assert(q->w_id == id + 1);
#if SHARED_NOTHING
     /* we have to acquire all partition locks in warehouse order.  */
     char bits[BITNSLOTS(MAX_SERVERS)];
     int i, alock_state, partitions[MAX_SERVERS], npartitions;

     memset(bits, 0, sizeof(bits));

     //we should aquire the lock for our warehouse
     BITSET(bits, w_id);
     //we should also lock other partitions => this is the challenge
     //we only access one partition
     //the first bit in bits array must be zero because ???
     assert(BITTEST(bits, 0) == 0);
     //we have to save the number of partitions because we need it when we want to free the partitions
     npartitions = 0;
     //go on the servers one by one and lock the partitons for which we have a 1 bit in the bits array
     for (i = 1; i <= g_nservers; i++) {
       if (BITTEST(bits, i)) {
         partitions[npartitions++] = i;
         LATCH_ACQUIRE(&hash_table->partitions[i - 1].latch, &alock_state);
       }
     }

#endif
   txn_start(hash_table, id, ctx);

   int current_time = time(NULL);
   int no_o_id;
   int c_id;

   for(int d_id = 1 ; d_id <= TPCC_NDIST_PER_WH ; d_id++){

       for (int o = 1; o <= TPCC_NCUST_PER_DIST; o++){
           no_o_id = o;
           key = MAKE_CUST_KEY(w_id, d_id, o);
           pkey = MAKE_HASH_KEY(NEW_ORDER_TID, key);
           MAKE_OP(op, OPTYPE_LOOKUP, 0, pkey);
           struct tpcc_order *no_r =
            (struct tpcc_order *) txn_op(ctask, hash_table, id, &op, w_id - 1);
          if(no_r){
            break;
          }
       }

       /**/key = MAKE_CUST_KEY(w_id, d_id, no_o_id);
       pkey = MAKE_HASH_KEY(ORDER_TID, key);
       MAKE_OP(op, OPTYPE_UPDATE, 0, pkey);
       assert(op.optype == OPTYPE_UPDATE);
       struct tpcc_order * o_r =
         (struct tpcc_order *) txn_op(ctask, hash_table, id, &op, w_id - 1);
       CHK_ABORT(o_r);
       assert(q->o_carrier_id);
       o_r->o_carrier_id = q->o_carrier_id;
       c_id = o_r->o_c_id;
      /*
       key = MAKE_CUST_KEY(w_id, d_id, no_o_id);
       pkey = MAKE_HASH_KEY(NEW_ORDER_TID, key);
       MAKE_OP(op, OPTYPE_DELETE, 0, pkey);
       struct tpcc_order *no_r =
         (struct tpcc_order *) txn_op(ctask, hash_table, id, &op, w_id - 1);
      if(no_r){
        printf("srv(%d): deleted %"PRId64"\n", id, pkey);
        printf("w_id = %d d_id = %d o_id = %d\n",w_id,d_id,no_o_id);
      }
         */
    long sum = 0;
    for (int ol_number = 5 ; ol_number <= TPCC_MAX_OL_PER_ORDER ; ol_number++){

       key = MAKE_OL_KEY(w_id, d_id, no_o_id, ol_number);
       pkey = MAKE_HASH_KEY(ORDER_LINE_TID, key);
       MAKE_OP(op, OPTYPE_UPDATE, 0, pkey);

         struct tpcc_order_line *ol_r =
           (struct tpcc_order_line *) txn_op(ctask, hash_table, id, &op, w_id - 1);
           if(ol_r != NULL){
             ol_r->ol_delivery_d = current_time;
             sum = sum + ol_r->ol_amount;
           }

     }

    key = MAKE_CUST_KEY(w_id, d_id, c_id);
    pkey = MAKE_HASH_KEY(CUSTOMER_TID, key);
    MAKE_OP(op, OPTYPE_UPDATE, 0, pkey);
      struct tpcc_customer *c_r =
        (struct tpcc_customer *) txn_op(ctask, hash_table, id, &op, w_id - 1);
        CHK_ABORT(c_r);
        if(c_r != NULL){
          c_r->c_balance = c_r->c_balance + sum;
        }

 }
   final:
   #if SHARED_NOTHING
     /* in shared nothing case, there is now way a transaction will ever be
      * aborted. We always get all partition locks before we start. So txn
      * execution is completely serial. Likewise, there is no need to reset
      * reference counts in records for logical locks as we don't need them.
      *
      * So just unlatch the partitions and be done
      */

     assert (r == TXN_COMMIT);
     for (i = 0; i < npartitions; i++) {
       int t = partitions[i];
       assert (BITTEST(bits, t));
       LATCH_RELEASE(&hash_table->partitions[t - 1].latch, &alock_state);
     }

   #else
   if (r == TXN_COMMIT)
     r = txn_commit(ctask, hash_table, id, TXN_SINGLE);
   else
     txn_abort(ctask, hash_table, id, TXN_SINGLE);
   #endif
     return r;

   }

   int tpcc_run_stocklevel_txn(struct hash_table *hash_table, int id,
      struct tpcc_query *q, struct task *ctask)
  {
	/*
	This transaction accesses
	order_line,
	stock,
	district
	*/

  struct hash_op op;
  uint64_t key, pkey;
  int w_id = q->w_id;
  int d_id = q->d_id;
  short opids[MAX_OPS_PER_QUERY];

  int r = TXN_COMMIT;

  //assert(q->w_id == id + 1);
  #if SHARED_NOTHING
    /* we have to acquire all partition locks in warehouse order.  */
    char bits[BITNSLOTS(MAX_SERVERS)];
    int i, alock_state, partitions[MAX_SERVERS], npartitions;

    memset(bits, 0, sizeof(bits));

    //we should aquire the lock for our warehouse
    BITSET(bits, w_id);
    //we should also lock other partitions => this is the challenge
    //It accesses a single partition
    //the first bit in bits array must be zero because ???
    assert(BITTEST(bits, 0) == 0);
    //we have to save the number of partitions because we need it when we want to free the partitions
    npartitions = 0;
    //go on the servers one by one and lock the partitons for which we have a 1 bit in the bits array
    for (i = 1; i <= g_nservers; i++) {
      if (BITTEST(bits, i)) {
        partitions[npartitions++] = i;
        LATCH_ACQUIRE(&hash_table->partitions[i - 1].latch, &alock_state);

      }
    }
  #endif

    /*
     * EXEC SQL SELECT d_next_o_id INTO :o_id
     * FROM district
     * WHERE d_w_id=:w_id AND d_id=:d_id;
     */
     key = MAKE_DIST_KEY(w_id, d_id);
     pkey = MAKE_HASH_KEY(DISTRICT_TID, key);
     MAKE_OP(op, OPTYPE_LOOKUP, 0, pkey);
     assert(op.optype == 0x00000000);
     struct tpcc_district *d_r =
       (struct tpcc_district *) txn_op(ctask, hash_table, id, &op, w_id - 1);

    CHK_ABORT(d_r);

     int64_t o_id = d_r->d_next_o_id;

     /*
     * EXEC SQL SELECT COUNT(DISTINCT (s_i_id)) INTO :stock_count
     * FROM order_line, stock
     * WHERE ol_w_id=:w_id AND
     * ol_d_id=:d_id AND ol_o_id<:o_id AND
     * ol_o_id>=:o_id-20 AND s_w_id=:w_id AND
     * s_i_id=ol_i_id AND s_quantity < :threshold;
     */

   int64_t ol_o_id = o_id - 20;
     int ol_number = 1;
     int stock_count = 0;
     int ol_i_id;
     while(ol_o_id < o_id){
       while(ol_number < 21){

        key = MAKE_OL_KEY(w_id, d_id, ol_o_id, ol_number);
        pkey = MAKE_HASH_KEY(ORDER_LINE_TID, key);
        MAKE_OP(op, OPTYPE_LOOKUP, 0, pkey);
        assert(op.optype == 0x00000000);

          struct tpcc_order_line *ol_r =
            (struct tpcc_order_line *) txn_op(ctask, hash_table, id, &op, w_id - 1);
            if(ol_r){
                ol_i_id = ol_r->ol_i_id;
            }
            key = MAKE_STOCK_KEY(w_id, ol_o_id);
            pkey = MAKE_HASH_KEY(STOCK_TID, key);
            MAKE_OP(op, OPTYPE_LOOKUP, 0, pkey);
            if(op.key > STOCK_TID_DEC + (w_id + 1) * TPCC_MAX_ITEMS){
              struct tpcc_stock *s_r =
                (struct tpcc_stock *) txn_op(ctask, hash_table, id, &op, w_id - 1);
              CHK_ABORT(s_r);

              if(s_r->s_quantity < q->threshold){
                  stock_count++;
              }
            }

          ol_number = ol_number + 1;
      }
      ol_o_id = ol_o_id + 1;
    }

  final:
  #if SHARED_NOTHING
    /* in shared nothing case, there is now way a transaction will ever be
     * aborted. We always get all partition locks before we start. So txn
     * execution is completely serial. Likewise, there is no need to reset
     * reference counts in records for logical locks as we don't need them.
     *
     * So just unlatch the partitions and be done
     */

    assert (r == TXN_COMMIT);
    for (i = 0; i < npartitions; i++) {
      int t = partitions[i];
      assert (BITTEST(bits, t));
      LATCH_RELEASE(&hash_table->partitions[t - 1].latch, &alock_state);
    }

  #else
  if (r == TXN_COMMIT)
    r = txn_commit(ctask, hash_table, id, TXN_SINGLE);
  else
    txn_abort(ctask, hash_table, id, TXN_SINGLE);
  #endif

    return r;
   }


void *tpcc_alloc_query()
{
  struct tpcc_query *q = malloc(sizeof(struct tpcc_query));
  assert(q);

  return (void *)q;
}

void tpcc_free_query(void *p)
{
  struct tpcc_query *q = (struct tpcc_query *)p;
  assert(q);

  free (q);
}

int tpcc_run_txn(struct hash_table *hash_table, int s, void *arg,
    struct task *ctask)
{

  int r;
  struct tpcc_query *q = (struct tpcc_query *) arg;
  // if we're running tpcc, se_index_latch better be enabled
#if defined SHARED_EVERYTHING
#ifndef SE_LATCH
  printf("Can't run tpcc in shared everything mode without index latch\n");
  exit(1);
#endif
#endif

  //tnx input is for transaction selection:
  //1 => new orders
  //2 => payement
  //3 => order status
  //4 => delivery
  //5 => stock level
  #if GATHER_STATS
  count_tpcc_transaction(hash_table, s, hash_table->partitions[s].selected_transaction);
  #endif
  switch (hash_table->partitions[s].selected_transaction){
    case 1:
     return tpcc_run_neworder_txn(hash_table, s, q, ctask);
     break;
    case 2:
     return tpcc_run_payment_txn(hash_table, s, q, ctask);
     break;
    case 3:
     return tpcc_run_orderstatus_txn(hash_table, s, q, ctask);
     break;
    case 4:
     return tpcc_run_delivery_txn(hash_table, s, q, ctask);
     break;
    case 5:
     return tpcc_run_stocklevel_txn(hash_table, s, q, ctask);
     break;
   default:
     printf("invalid transaction is selected %d\n", hash_table->partitions[s].selected_transaction);
     assert(0);
  }

  return r;
}

void tpcc_get_next_query(struct hash_table *hash_table, int s,
    void *arg)
{

  hash_table->partitions[s].q_idx++;
  switch( hash_table->partitions[s].selected_transaction){
    case 1:
      tpcc_get_next_neworder_query(hash_table, s, arg);
      break;
    case 2:
      tpcc_get_next_payment_query(hash_table, s, arg);
      break;
    case 3:
      tpcc_get_next_orderstatus_query(hash_table, s, arg);
      break;
    case 4:
      tpcc_get_next_delivery_query(hash_table, s, arg);
      break;
    case 5:
      tpcc_get_next_stocklevel_query(hash_table, s, arg);
      break;
    default:
      printf("Undefined transaction * %d",hash_table->partitions[s].selected_transaction);
      assert(0);
  }
}

/**/void tpcc_verify_txn(struct hash_table *hash_table, int id)
{

  hash_key key, pkey;
#if SHARED_EVERYTHING
  struct partition *p = &hash_table->partitions[0];
#else
  struct partition *p = &hash_table->partitions[id];
#endif
  struct elem *e;
  int w_id = id + 1;

  printf("Server %d verifying consistency..\n", id);

  // check 1: w_ytd = sum(d_ytd)
  pkey = MAKE_HASH_KEY(WAREHOUSE_TID, w_id);
  e = hash_lookup(p, pkey);
  assert(e);

  struct tpcc_warehouse *w_r = (struct tpcc_warehouse *)e->value;
  assert(w_r);

  double d_ytd = 0;
  for (int d = 1; d <= TPCC_NDIST_PER_WH; d++) {
    key = MAKE_DIST_KEY(w_id, d);
    pkey = MAKE_HASH_KEY(DISTRICT_TID, key);

    struct elem *e = hash_lookup(p, pkey);
    assert(e);

    struct tpcc_district *d_r = (struct tpcc_district *)e->value;
    assert(d_r);

    d_ytd += d_r->d_ytd;

  }

  assert(d_ytd == w_r->w_ytd);

  // with one global sweep of hashtable, get all necessary values
  // check 2 vars
  int max_o_id[TPCC_NDIST_PER_WH + 1], max_no_o_id[TPCC_NDIST_PER_WH + 1];

  // check 3 vars
  int nrows_no[TPCC_NDIST_PER_WH + 1], min_no_o_id[TPCC_NDIST_PER_WH + 1];

  //check 4 vars
  int sum_o_ol_cnt[TPCC_NDIST_PER_WH + 1], nrows_ol[TPCC_NDIST_PER_WH + 1];

  for (int i = 0; i <= TPCC_NDIST_PER_WH; i++) {
    nrows_no[i] = max_o_id[i] = max_no_o_id[i] = 0;
    min_no_o_id[i] = INT_MAX - 1;
    sum_o_ol_cnt[i] = nrows_ol[i] = 0;
  }

  for (int i = 0; i < p->nhash; i++) {
    struct elist *eh = &(p->table[i].chain);
    struct elem *e = LIST_FIRST(eh);

    while (e != NULL) {
      hash_key key = e->key;
      uint64_t tid = GET_TID(key);
      if (tid == ORDER_TID) {
        struct tpcc_order *o_r = (struct tpcc_order *)e->value;
        assert(o_r);

#ifndef SHARED_EVERYTHING
        assert (o_r->o_w_id == w_id);
#else
        // shared everything config
        if (o_r->o_w_id != w_id) {
          e = LIST_NEXT(e, chain);
          continue;
        }
#endif

        if (max_o_id[o_r->o_d_id] < o_r->o_id)
          max_o_id[o_r->o_d_id] = o_r->o_id;

        sum_o_ol_cnt[o_r->o_d_id] += o_r->o_ol_cnt;

      } else if (tid == NEW_ORDER_TID) {
        struct tpcc_new_order *no_r = (struct tpcc_new_order *)e->value;
        assert(no_r);

#ifndef SHARED_EVERYTHING
        assert (no_r->no_w_id == w_id);
#else
        // shared everything config
        if (no_r->no_w_id != w_id) {
          e = LIST_NEXT(e, chain);
          continue;
        }
#endif

        nrows_no[no_r->no_d_id]++;

        if (max_no_o_id[no_r->no_d_id] < no_r->no_o_id)
          max_no_o_id[no_r->no_d_id] = no_r->no_o_id;

        if (min_no_o_id[no_r->no_d_id] > no_r->no_o_id)
          min_no_o_id[no_r->no_d_id] = no_r->no_o_id;

     } else if (tid == ORDER_LINE_TID) {
        struct tpcc_order_line *ol_r = (struct tpcc_order_line *)e->value;
        assert(ol_r);

#ifndef SHARED_EVERYTHING
        assert (ol_r->ol_w_id == w_id);
#else
        // shared everything config
        if (ol_r->ol_w_id != w_id) {
          e = LIST_NEXT(e, chain);
          continue;
        }
#endif

        nrows_ol[ol_r->ol_d_id]++;
      }

      e = LIST_NEXT(e, chain);
    }
  }

  for (int d = 1; d <= TPCC_NDIST_PER_WH; d++) {
    key = MAKE_DIST_KEY(w_id, d);
    pkey = MAKE_HASH_KEY(DISTRICT_TID, key);

    struct elem *e = hash_lookup(p, pkey);
    assert(e);

    struct tpcc_district *d_r = (struct tpcc_district *)e->value;
    assert(d_r);

    // check 2: D_NEXT_O_ID - 1 = max(O_ID) = max(NO_O_ID)
    assert((d_r->d_next_o_id - 1) == max_o_id[d]);

    //check 4: sum (O_OL_CNT) = number of rows in the ORDER-LINE table*/
    assert(sum_o_ol_cnt[d] == nrows_ol[d]);
  }
}/**/

static int fetch_cust_records(struct hash_table *hash_table, int id,
    struct task *ctask, struct tpcc_customer **c_recs,
    struct secondary_record *sr, struct tpcc_query *q,
    short *opids, short *nopids)
{
  /*==========================================================================+
    EXEC SQL DECLARE c_byname CURSOR FOR
    SELECT c_first, c_middle, c_id, c_street_1, c_street_2, c_city, c_state,
      c_zip, c_phone, c_credit, c_credit_lim, c_discount, c_balance, c_since
    FROM customer
    WHERE c_w_id=:c_w_id AND c_d_id=:c_d_id AND c_last=:c_last
    ORDER BY c_first;
    EXEC SQL OPEN c_byname;
    +===========================================================================*/
  int i, nmatch;
  struct hash_op op;
  int sr_nids = sr->sr_idx;
  int w_id = q->w_id;
  int c_w_id = q->c_w_id;
  short opid_idx = *nopids;

  i = nmatch = 0;
  while (i < sr_nids) {
    /* XXX: Painful here. We have to retrieve all customers with update lock
     * as we potentially anybody could be the median guy. We might need lock
     * escalation if this becomes a bottleneck in practice
     *
     * XXX: We are requesting one by one. We can batch all requests here
     */
    hash_key pkey = MAKE_HASH_KEY(CUSTOMER_TID, sr->sr_rids[i]);
    MAKE_OP(op, OPTYPE_UPDATE, 0, pkey);

    struct tpcc_customer *tmp = NULL;

    tmp = (struct tpcc_customer *) txn_op(ctask, hash_table, id, &op, c_w_id - 1);
    opids[opid_idx++] = 0;
    *nopids = opid_idx;

    if (!tmp)
      return -1;

    /* XXX: This strcmp and the next below have a huge overhead */
    if (strcmp(tmp->c_last, q->c_last) == 0) {
      c_recs[nmatch++] = tmp;
    }

    i++;
  }

  return nmatch;
}

static int batch_fetch_cust_records(struct hash_table *hash_table, int id,
    struct task *ctask, struct tpcc_customer **c_recs,
    struct secondary_record *sr, struct tpcc_query *q,
    short *opids, short *nopids)
{
  /*==========================================================================+
    EXEC SQL DECLARE c_byname CURSOR FOR
    SELECT c_first, c_middle, c_id, c_street_1, c_street_2, c_city, c_state,
      c_zip, c_phone, c_credit, c_credit_lim, c_discount, c_balance, c_since
    FROM customer
    WHERE c_w_id=:c_w_id AND c_d_id=:c_d_id AND c_last=:c_last
    ORDER BY c_first;
    EXEC SQL OPEN c_byname;
    +===========================================================================*/
  int i, nmatch;
  struct hash_op op;
  int sr_nids = sr->sr_idx;
  int w_id = q->w_id;
  int c_w_id = q->c_w_id;
  struct txn_ctx *ctx = &ctask->txn_ctx;
  struct partition *p = &hash_table->partitions[id];
  short opid_idx = *nopids;

  dprint("srv(%d): batch fetching %d cust records from %d\n", id, sr_nids,
      c_w_id - 1);

  assert(c_w_id != w_id);

  assert(sr_nids < MAX_OPS_PER_QUERY);

  for (int i = 0; i < sr_nids; i++) {
    hash_key pkey = MAKE_HASH_KEY(CUSTOMER_TID, sr->sr_rids[i]);

    /* XXX: we are setting opid to 0 here and this should be fine. This is
     * because, we are sending all requests to the same server. As the
     * server will process requests , we will  get response back in same
     * order as we requested them. So no need for opids
     */
    smp_hash_update(ctask, hash_table, id, c_w_id - 1, pkey, i);

    c_recs[i] = NULL;
  }

  ctask->npending = sr_nids;
  ctask->nresponses = 0;

  smp_flush_all(hash_table, id);

  task_yield(p, TASK_STATE_WAITING);

  assert(ctask->nresponses == sr_nids);

  int r = TXN_COMMIT;
  nmatch = 0;
  for (int i = 0; i < sr_nids; i++) {
    uint64_t val = ctask->received_responses[i];
    assert(HASHOP_GET_TID(val) == ctask->tid);

    short opid = HASHOP_GET_OPID(val);
    assert(opid < sr_nids);

    opids[opid_idx++] = opid;

    struct op_ctx *octx = &ctx->op_ctx[ctx->nops];
    octx->optype = OPTYPE_UPDATE;
    octx->e = (struct elem *)HASHOP_GET_VAL(val);
    octx->target = c_w_id - 1;
    ctx->nops++;

    if (octx->e) {
      assert(octx->e->key == sr->sr_rids[opid]);
      assert(octx->e->ref_count & DATA_READY_MASK);
      int esize = octx->e->size;
      octx->data_copy = plmalloc_alloc(p, esize);
      memcpy(octx->data_copy, octx->e->value, esize);

      // is this cust record a match?
      struct tpcc_customer *tmp = (struct tpcc_customer *) octx->e->value;
      if (strcmp(tmp->c_last, q->c_last) == 0)
        c_recs[nmatch++] = tmp;

    } else {
      r = TXN_ABORT;
    }
  }

  *nopids = opid_idx;

  return r == TXN_COMMIT ? nmatch : -1;
}

struct benchmark tpcc_bench = {
  .init = tpcc_init,
  .alloc_query = tpcc_alloc_query,
  .load_data = tpcc_load_data,
  .get_next_query = tpcc_get_next_query,
  .run_txn = tpcc_run_txn,
  .verify_txn = tpcc_verify_txn,
};
