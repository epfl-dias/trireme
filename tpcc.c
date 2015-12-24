#include <stdio.h>
#include <string.h>
#include <assert.h>
#include <stdlib.h>
#include <limits.h>
#include <inttypes.h>
#include "hashprotocol.h"
#include "onewaybuffer.h"
#include "partition.h"
#include "smphashtable.h"
#include "util.h"
#include "benchmark.h"
#include "tpcc.h"

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

  // payment input
  int d_w_id;
  int c_w_id;
  int c_d_id;
  char c_last[LAST_NAME_LEN];
  double h_amount;
  char by_last_name;

  //new order input
  struct item item[TPCC_MAX_OL_PER_ORDER];
  char rbk;
  char remote;
  int ol_cnt;
  int o_entry_d;
};

extern double dist_threshold;

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

int tpcc_hash_get_server(struct hash_table *hash_table, hash_key key)
{
  // only time we lookup remotely is for stock table
  uint64_t tid = key & TID_MASK;
  int target;
  hash_key base_cid;

  assert(tid == STOCK_TID || tid == CUSTOMER_SIDX_TID || tid == CUSTOMER_TID);

  switch (tid) {
    case STOCK_TID:
      target = ((key - 1) & ~TID_MASK) / TPCC_MAX_ITEMS - 1;
      break;

    case CUSTOMER_SIDX_TID:
      // XXX: ugly hack. key decoding here depends on the string hashing
      // in cust_derive_key. We just get lower 10 bits and derive w_id
      base_cid = TPCC_NDIST_PER_WH + 1;
      key = key & ~TID_MASK & 0x3FF;
      key -= base_cid;
      assert(key >= 0);

      target = key / TPCC_NDIST_PER_WH;
      break;

    case CUSTOMER_TID:
      // distid, wid, all start from 1. So starting cid is 33001
      base_cid = MAKE_CUST_KEY(1,1,1);
      key = key & ~TID_MASK;
      key -= base_cid;

      target = key / (TPCC_NCUST_PER_DIST * TPCC_NDIST_PER_WH);

      break;

    default:
      printf("Invalid TID with key %"PRId64"\n", key);
      assert(0);
  }

  assert(target >= 0);

  return target;
}

void tpcc_get_next_payment_query(struct hash_table *hash_table, int s,
    void *arg)
{
  struct partition *p = &hash_table->partitions[s];
  struct tpcc_query *q = (struct tpcc_query *) arg;

  q->w_id = s + 1;
  q->d_w_id = s + 1;
  q->d_id = URand(&p->seed, 1, TPCC_NDIST_PER_WH);
  q->h_amount = URand(&p->seed, 1, 5000);
  int x = URand(&p->seed, 1, 100);
  int y = URand(&p->seed, 1, 100);

  //XXX: For now, always home wh
  //if (1) {
  if(x <= 85 || dist_threshold == 1) {
    // home warehouse
    q->c_d_id = q->d_id;
    q->c_w_id = s + 1;
  } else {
    q->c_d_id = URand(&p->seed, 1, TPCC_NDIST_PER_WH);

    // remote warehouse if we have >1 wh
    if(p->nservers > 1) {
      while((q->c_w_id = URand(&p->seed, 1, p->nservers)) == (s + 1))
        ; 

    } else {
      q->c_w_id = s + 1;
    }
  }

  if(y <= 60) {
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

  // XXX: for now, only neworder query. for now, only local
  q->w_id = s + 1;
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
    if (dist_threshold == 1)
      x = 2;

    if (x > 1 || p->nservers == 1) {
    //if (1) {
      i->ol_supply_w_id = s + 1;
    } else {
      while ((i->ol_supply_w_id = URand(&p->seed, 1, p->nservers)) == (s + 1))
        ;

      q->remote = 1;
    }

    assert(i->ol_supply_w_id != 0);

    i->ol_quantity = URand(&p->seed, 1, 10);
  }
}
    
__attribute__ ((unused)) static uint64_t cust_derive_key(char * c_last, 
    int c_d_id, int c_w_id)
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

  if (w_id != 1) {
    printf("Client %d skipping loading item table\n", w_id);
  }
    
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

void tpcc_load_data(struct hash_table *hash_table, int id)
{
  struct partition *p = &hash_table->partitions[id];
  struct partition *item = hash_table->g_partition;
  int w_id = id + 1;

  printf("srv (%d): Loading stock..\n", id);
  load_stock(w_id, p);

  printf("srv (%d): Loading history..\n", id);
  load_history(w_id, p);

  printf("srv (%d): Loading customer..\n", id);
  load_customer(w_id, p);

  printf("srv (%d): Loading order..\n", id);
  load_order(w_id, p);
  if (id == 0) {
    load_item(id, item);
    printf("srv (%d): Loading item..\n", id);
  }

  printf("srv (%d): Loading wh..\n", id);
  load_warehouse(w_id, p);

  printf("srv (%d): Loading district..\n", id);
  load_district(w_id, p);
}

int tpcc_run_neworder_txn(struct hash_table *hash_table, int id, 
    struct tpcc_query *q)
{
  uint64_t key, pkey;
  struct hash_op op;
  struct partition *item_p = hash_table->g_partition;
  char remote = q->remote;
  int w_id = q->w_id;
  int d_id = q->d_id;
  int c_id = q->c_id;
  int ol_cnt = q->ol_cnt;
  int o_entry_d = q->o_entry_d;

  int r = TXN_COMMIT;

  txn_start(hash_table, id);

  /*
   * EXEC SQL SELECT c_discount, c_last, c_credit, w_tax
   * INTO :c_discount, :c_last, :c_credit, :w_tax
   * FROM customer, warehouse
   * WHERE w_id = :w_id AND c_w_id = w_id AND c_d_id = :d_id AND c_id = :c_id;
   */
  pkey = MAKE_HASH_KEY(WAREHOUSE_TID, w_id);
  MAKE_OP(op, OPTYPE_LOOKUP, 0, pkey);
  struct tpcc_warehouse *w_r = 
    (struct tpcc_warehouse *) txn_op(hash_table, id, NULL, &op, 1 /*local*/);
  assert(w_r);

  double w_tax = w_r->w_tax;
  key = MAKE_CUST_KEY(w_id, d_id, c_id);
  pkey = MAKE_HASH_KEY(CUSTOMER_TID, key);
  MAKE_OP(op, OPTYPE_LOOKUP, 0, pkey);
  struct tpcc_customer *c_r = 
    (struct tpcc_customer *) txn_op(hash_table, id, NULL, &op, 1);
  assert(c_r);

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
    (struct tpcc_district *) txn_op(hash_table, id, NULL, &op, 1);
  assert(d_r);

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
    (struct tpcc_order *) txn_op(hash_table, id, NULL, &op, 1);
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
    (struct tpcc_new_order *) txn_op(hash_table, id, NULL, &op, 1);
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
      (struct tpcc_item *) txn_op(hash_table, id, 
        item_p, &op, 1);
    assert(i_r);

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

    struct tpcc_stock *s_r = 
      (struct tpcc_stock *) txn_op(hash_table, id, NULL, &op, w_id == ol_supply_w_id);
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
      (struct tpcc_order_line *) txn_op(hash_table, id, NULL, &op, 1);
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
  if (r == TXN_COMMIT)
    txn_commit(hash_table, id, TXN_SINGLE);
  else
    txn_abort(hash_table, id, TXN_SINGLE);
  
  return r;
}

int tpcc_run_payment_txn (struct hash_table *hash_table, int id, struct tpcc_query *q)
{
  struct hash_op op;
  uint64_t key, pkey;
  int w_id = q->w_id;
  int d_id = q->d_id;
  int c_w_id = q->c_w_id;
  int c_d_id = q->c_d_id;
  int c_id = q->c_id;
  double h_amount = q->h_amount;

  int r = TXN_COMMIT;

  assert(q->w_id == id + 1);

  txn_start(hash_table, id);

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
  struct tpcc_warehouse *w_r = 
    (struct tpcc_warehouse *) txn_op(hash_table, id, NULL, &op, 1 /*local*/);
  assert(w_r);

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
  struct tpcc_district *d_r = 
    (struct tpcc_district *) txn_op(hash_table, id, NULL, &op, 1);
  assert(d_r);

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
    struct secondary_record *sr = 
      (struct secondary_record *) txn_op(hash_table, id, NULL, &op, c_w_id == w_id);
    assert(sr);
 
    struct tpcc_customer **c_recs = malloc(sizeof(struct tpcc_customer *) * 
      sr->sr_nids);
    assert(c_recs);

    /*==========================================================================+
      EXEC SQL DECLARE c_byname CURSOR FOR
      SELECT c_first, c_middle, c_id, c_street_1, c_street_2, c_city, c_state,
        c_zip, c_phone, c_credit, c_credit_lim, c_discount, c_balance, c_since
        FROM customer
        WHERE c_w_id=:c_w_id AND c_d_id=:c_d_id AND c_last=:c_last
        ORDER BY c_first;
      EXEC SQL OPEN c_byname;
    +===========================================================================*/
    /* retrieve all matching records */
    int i, nmatch;
    i = nmatch = 0;
    int sr_nids = sr->sr_idx;
    while (i < sr_nids) {
      /* XXX: Painful here. We have to retrieve all customers with update lock
       * as we potentially anybody could be the median guy. We might need lock
       * escalation if this becomes a bottleneck in practice
       * 
       * XXX: We are requesting one by one. We can batch all requests here
       */
      pkey = MAKE_HASH_KEY(CUSTOMER_TID, sr->sr_rids[i]);
      MAKE_OP(op, OPTYPE_LOOKUP, 0, pkey);

      struct tpcc_customer *tmp = 
        (struct tpcc_customer *) txn_op(hash_table, id, NULL, &op, c_w_id == w_id);
      assert(tmp);

      /* XXX: This strcmp and the next below have a huge overhead */
      if (strcmp(tmp->c_last, q->c_last) == 0) {
        c_recs[nmatch++] = tmp;
      }

      i++;
    }

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

    free(c_recs);

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

      c_r = (struct tpcc_customer *) txn_op(hash_table, id, NULL, &op, c_w_id == w_id);
      assert(c_r);
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
    struct tpcc_history *h_r = 
      (struct tpcc_history *) txn_op(hash_table, id, NULL, &op, 1);
    assert(h_r);

    h_r->h_c_id = c_id;
    h_r->h_c_d_id = c_d_id;
    h_r->h_c_w_id = c_w_id;
    h_r->h_d_id = d_id;
    h_r->h_w_id = w_id;
    h_r->h_date = 2013; /* XXX: Why 2013? */
    h_r->h_amount = h_amount;

    if (r == TXN_COMMIT)
      txn_commit(hash_table, id, TXN_SINGLE);
    else
      txn_abort(hash_table, id, TXN_SINGLE);

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

int tpcc_run_txn(struct hash_table *hash_table, int s, void *arg)
{
  int r;
  struct tpcc_query *q = (struct tpcc_query *) arg;

  //r = tpcc_run_neworder_txn(hash_table, s, q); 
  r =  tpcc_run_payment_txn(hash_table, s, q); 

  return r;
}

void tpcc_get_next_query(struct hash_table *hash_table, int s, 
    void *arg)
{
  //tpcc_get_next_neworder_query(hash_table, s, arg);
  tpcc_get_next_payment_query(hash_table, s, arg);
}

void tpcc_verify_txn(struct hash_table *hash_table, int id)
{

  hash_key key, pkey;
  struct partition *p = &hash_table->partitions[id];
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
    struct elem *e = TAILQ_FIRST(eh);

    while (e != NULL) {
      hash_key key = e->key;
      uint64_t tid = GET_TID(key);
      if (tid == ORDER_TID) {
        struct tpcc_order *o_r = (struct tpcc_order *)e->value;
        assert(o_r);

        if (max_o_id[o_r->o_d_id] < o_r->o_id)
          max_o_id[o_r->o_d_id] = o_r->o_id;

        sum_o_ol_cnt[o_r->o_d_id] += o_r->o_ol_cnt;

      } else if (tid == NEW_ORDER_TID) {
        struct tpcc_new_order *no_r = (struct tpcc_new_order *)e->value;
        assert(no_r);

        nrows_no[no_r->no_d_id]++;

        if (max_no_o_id[no_r->no_d_id] < no_r->no_o_id)
          max_no_o_id[no_r->no_d_id] = no_r->no_o_id;
        
        if (min_no_o_id[no_r->no_d_id] > no_r->no_o_id)
          min_no_o_id[no_r->no_d_id] = no_r->no_o_id;

     } else if (tid == ORDER_LINE_TID) {
        struct tpcc_order_line *ol_r = (struct tpcc_order_line *)e->value;
        assert(ol_r);

        nrows_ol[ol_r->ol_d_id]++;
      }

      e = TAILQ_NEXT(e, chain);
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
    assert((d_r->d_next_o_id - 1) == max_no_o_id[d]);

    // check 3: max(NO_O_ID) - min(NO_O_ID) + 1 = [number of rows in the NEW-ORDER
    // table for this district]
    assert((max_no_o_id[d] - min_no_o_id[d] + 1) == nrows_no[d]);

    //check 4: sum (O_OL_CNT) = number of rows in the ORDER-LINE table
    assert(sum_o_ol_cnt[d] == nrows_ol[d]);

    //check 5: 
  }

}

struct benchmark tpcc_bench = {
  .alloc_query = tpcc_alloc_query,
  .load_data = tpcc_load_data,
  .get_next_query = tpcc_get_next_query,
  .run_txn = tpcc_run_txn,
  .verify_txn = tpcc_verify_txn,
  .hash_get_server = tpcc_hash_get_server,
};
