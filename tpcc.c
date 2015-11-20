#include <stdio.h>
#include <string.h>
#include <assert.h>
#include <stdlib.h>
#include <inttypes.h>
#include "hashprotocol.h"
#include "onewaybuffer.h"
#include "partition.h"
#include "smphashtable.h"
#include "util.h"
#include "benchmark.h"
#include "tpcc.h"

struct item {
  uint64_t ol_i_id;
  uint64_t ol_supply_w_id;
  uint64_t ol_quantity;
};

// neworder tpcc query
struct tpcc_query {
  uint64_t w_id;
  uint64_t d_id;
  uint64_t c_id;
  struct item item[TPCC_MAX_OL_PER_ORDER];
  char rbk;
  char remote;
  uint64_t ol_cnt;
  uint64_t o_entry_d;
};

int tpcc_hash_get_server(struct hash_table *hash_table, hash_key key)
{
  // only time we lookup remotely is for stock table
  assert((key & TID_MASK) == STOCK_TID);

  return ((key - 1) & ~TID_MASK) / TPCC_MAX_ITEMS;
}

void tpcc_get_next_query(struct hash_table *hash_table, int s, void *arg)
{
  struct partition *p = &hash_table->partitions[s];
  int ol_cnt, dup;
  struct tpcc_query *q = (struct tpcc_query *) arg;

  // XXX: for now, only neworder query. for now, only local
  q->w_id = s;
  q->d_id = URand(1, TPCC_NDIST_PER_WH);
  q->c_id = NURand(1023, 1, TPCC_NCUST_PER_DIST);
  q->rbk = URand(1, 100);
  q->ol_cnt = URand(5, 15);
  q->o_entry_d = 2013;
  q->remote = 0;

  ol_cnt = q->ol_cnt;
  assert(ol_cnt <= TPCC_MAX_OL_PER_ORDER);
  for (int o = 0; o < ol_cnt; o++) {
    struct item *i = &q->item[o];

    do {
      i->ol_i_id = NURand(8191, 1, TPCC_MAX_ITEMS);

      // no duplicates
      dup = 0;
      for (int j = 0; j < o; j++)
        if (q->item[j].ol_i_id == i->ol_i_id) {
          dup = 1;
          break;
        }
    } while(dup);     

    int x = URand(1, 100);
    if (x > 1 || p->nservers == 1) {
    //if (1) {
      i->ol_supply_w_id = s;
    } else {
      while ((i->ol_supply_w_id = URand(0, p->nservers - 1) == s))
        ;
      
      q->remote = 1;
    }

    i->ol_quantity = URand(1, 10);
  }


}
    
static uint64_t set_last_name(uint64_t num, char* name)
{
  static const char *n[] =
  {"BAR", "OUGHT", "ABLE", "PRI", "PRES",
    "ESE", "ANTI", "CALLY", "ATION", "EING"};

  strcpy(name, n[num/100]); 
  strcat(name, n[(num/10)%10]);
  strcat(name, n[num%10]);
  return strlen(name);
}

__attribute__ ((unused)) static uint64_t cust_derive_key(char * c_last, 
    uint64_t c_d_id, uint64_t c_w_id)
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
    r->s_quantity = URand(10, 100);
    for (int j = 0; j < 10; j++)
      make_alpha_string(24, 24, r->s_dist[j]);

    r->s_ytd = 0;
    r->s_order_cnt = 0;
    r->s_remote_cnt = 0;
    
    int len = make_alpha_string(26, 50, r->s_data);
    if (RAND(100) < 10) {
      int idx = URand(0, len - 8); 
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
      make_alpha_string(12, 24, r->h_data);

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

      r = (struct tpcc_customer *) e->value;
      r->c_id = c;
      r->c_d_id = d;
      r->c_w_id = w_id;

      if (c <= 1000)
        set_last_name(c - 1, r->c_last);
      else
        set_last_name(NURand(255,0,999), r->c_last);

      memcpy(r->c_middle, "OE", 2);

      make_alpha_string(FIRST_NAME_MIN_LEN, FIRST_NAME_LEN, r->c_first);

      make_alpha_string(10, 20, r->c_street[0]);
      make_alpha_string(10, 20, r->c_street[1]);
      make_alpha_string(10, 20, r->c_city);
      make_alpha_string(2, 2, r->c_state); /* State */
      make_numeric_string(9, 9, r->c_zip); /* Zip */
      make_numeric_string(16, 16, r->c_phone); /* Zip */
      r->c_since = 0;
      r->c_credit_lim = 50000;
      r->c_delivery_cnt = 0;
      make_alpha_string(300, 500, r->c_data);

      if (RAND(10) == 0) {
        r->c_credit[0] = 'G';
      } else {
        r->c_credit[0] = 'B';
      }
      r->c_credit[1] = 'C';
      r->c_discount = (double)RAND(5000) / 10000;
      r->c_balance = -10.0;
      r->c_ytd_payment = 10.0;
      r->c_payment_cnt = 1;

      /* XXX: create secondary index using the main hash table itself. 
       * we can do this by deriving a key from the last name,dist,wh id 
       * and using it to create a new record which will contain both
       * the <last name,did,wid> secondary key and real key of the record
       */
#if 0
      hash_key dkey;
      dkey = cust_derive_key(r->c_last, d, w);

      // now we insert another record for secondary index
      struct elem *sie = hash_insert(p, dkey, 
          sizeof(struct secondary_record), NULL);
      assert(sie);
    
      struct secondary_record *sr = (struct secondary_record *)sie->value;
      strcpy(sr->sr_last_name, r->c_last);
      sr->sr_rid = r->ckey;
      sie->ref_count++;
#endif

      p->ninserts++;
      e->ref_count++;
    }
  }
}

void init_permutation(uint64_t *cperm)
{
    int i;

    for(i = 0; i < TPCC_NCUST_PER_DIST; i++) {
        cperm[i] = i+1;
    }

    // shuffle
    for(i = 0; i < TPCC_NCUST_PER_DIST - 1; i++) {
        uint64_t j = URand(i + 1, TPCC_NCUST_PER_DIST - 1);
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
    init_permutation(cperm); 

    for (int o = 1; o <= TPCC_NCUST_PER_DIST; o++) {
      ckey = MAKE_CUST_KEY(w_id, d, o);
      key = MAKE_HASH_KEY(ORDER_TID, ckey);

      e = hash_insert(p, key, sizeof(struct tpcc_order), NULL);
      assert(e);

      p->ninserts++;
      e->ref_count++;

      r = (struct tpcc_order *) e->value;

      uint64_t c_id = cperm[o - 1];
      r->o_id = o;
      r->o_c_id = c_id;
      r->o_d_id = d;
      r->o_w_id = w_id;
      uint64_t o_entry = 2013;
      r->o_entry_d = 2013;
      if (o < 2101)
        r->o_carrier_id = URand(1, 10);
      else
        r->o_carrier_id = 0;
      uint64_t o_ol_cnt = URand(5, 15);
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
        r_ol->ol_i_id = URand(1, 100000);
        r_ol->ol_supply_w_id = w_id;

        if (o < 2101) {
          r_ol->ol_delivery_d = o_entry;
          r_ol->ol_amount = 0;
        } else {
          r_ol->ol_delivery_d = 0;
          r_ol->ol_amount = (double)URand(1, 999999)/100;
        }
        r_ol->ol_quantity = 5;
        make_alpha_string(24, 24, r_ol->ol_dist_info);

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
    r->i_im_id = URand(1L,10000L);
    make_alpha_string(14, 24, r->i_name);
    r->i_price = URand(1, 100);
    data_len = make_alpha_string(26, 50, r->i_data);

    // TODO in TPCC, "original" should start at a random position
    if (RAND(10) == 0) {
      int idx = URand(0, data_len - 8);
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

  make_alpha_string(6, 10, r->w_name);
  make_alpha_string(10, 20, r->w_street[0]);
  make_alpha_string(10, 20, r->w_street[1]);
  make_alpha_string(10, 20, r->w_city);
  make_alpha_string(2, 2, r->w_state);
  make_alpha_string(9, 9, r->w_zip);
  double tax = (double)URand(0L,200L)/1000.0;
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

    make_alpha_string(6, 10, r->d_name);
    make_alpha_string(10, 20, r->d_street[0]);
    make_alpha_string(10, 20, r->d_street[1]);
    make_alpha_string(10, 20, r->d_city);
    make_alpha_string(2, 2, r->d_state);
    make_alpha_string(9, 9, r->d_zip);
    double tax = (double)URand(0L,200L)/1000.0;
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

  printf("srv (%d): Loading stock..\n", id);
  load_stock(id, p);

  printf("srv (%d): Loading history..\n", id);
  load_history(id, p);

  printf("srv (%d): Loading customer..\n", id);
  load_customer(id, p);

  printf("srv (%d): Loading order..\n", id);
  load_order(id, p);
  if (id == 0) {
    load_item(id, item);
    printf("srv (%d): Loading item..\n", id);
  }

  printf("srv (%d): Loading wh..\n", id);
  load_warehouse(id, p);

  printf("srv (%d): Loading district..\n", id);
  load_district(id, p);
}

int tpcc_run_neworder_txn_v1 (struct hash_table *hash_table, int id, 
    struct tpcc_query *q)
{
  uint64_t key, pkey;
  struct hash_op op;
  struct partition *item_p = hash_table->g_partition;
  char remote = q->remote;
  uint64_t w_id = q->w_id;
  uint64_t d_id = q->d_id;
  uint64_t c_id = q->c_id;
  uint64_t ol_cnt = q->ol_cnt;
  uint64_t o_entry_d = q->o_entry_d;

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
  d_r->d_next_o_id = o_id++;

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

  dprint("srv(%d): inserted %"PRId64"\n", id, pkey);

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

int tpcc_run_neworder_txn (struct hash_table *hash_table, int id, struct tpcc_query *q)
{
  uint64_t key, pkey;
  struct partition *p = &hash_table->partitions[id];
  struct partition *item_p = hash_table->g_partition;
  char remote = q->remote;
  uint64_t w_id = q->w_id;
  uint64_t d_id = q->d_id;
  uint64_t c_id = q->c_id;
  uint64_t ol_cnt = q->ol_cnt;
  uint64_t o_entry_d = q->o_entry_d;

  /*
   * EXEC SQL SELECT c_discount, c_last, c_credit, w_tax
   * INTO :c_discount, :c_last, :c_credit, :w_tax
   * FROM customer, warehouse
   * WHERE w_id = :w_id AND c_w_id = w_id AND c_d_id = :d_id AND c_id = :c_id;
   */
  pkey = MAKE_HASH_KEY(WAREHOUSE_TID, w_id);

  // we only support local lookups for now
  struct elem *w_e = hash_lookup(p, pkey);
  assert(w_e);

  struct tpcc_warehouse *w_r = (struct tpcc_warehouse *)w_e->value;
  assert(w_r);

  double w_tax = w_r->w_tax;
  key = MAKE_CUST_KEY(w_id, d_id, c_id);
  pkey = MAKE_HASH_KEY(CUSTOMER_TID, key);
  struct elem *c_e = hash_lookup(p, pkey);
  assert(c_e);

  struct tpcc_customer *c_r = (struct tpcc_customer *)c_e->value;
  assert(c_r);

  uint64_t c_discount = c_r->c_discount;

  /*
   * EXEC SQL SELECT d_next_o_id, d_tax
   *  INTO :d_next_o_id, :d_tax
   *  FROM district WHERE d_id = :d_id AND d_w_id = :w_id;
   */
  key = MAKE_DIST_KEY(w_id, d_id);
  pkey = MAKE_HASH_KEY(DISTRICT_TID, key);
  struct elem *d_e = hash_lookup(p, pkey);
  assert(d_e);

  struct tpcc_district *d_r = (struct tpcc_district *) d_e->value;
  assert(d_r);

  uint64_t o_id = d_r->d_next_o_id;
  double d_tax = d_r->d_tax;

  /* 
   * EXEC SQL UPDATE district SET d _next_o_id = :d _next_o_id + 1
   * WHERE d _id = :d_id AN D d _w _id = :w _id ; 
   */
  d_r->d_next_o_id = o_id++;

  /* 
   * EXEC SQL INSERT IN TO ORDERS (o_id , o_d _id , o_w _id , o_c_id ,
   * o_entry_d , o_ol_cnt, o_all_local)
   * VALUES (:o_id , :d _id , :w _id , :c_id ,
   * :d atetime, :o_ol_cnt, :o_all_local);
   */
  key = MAKE_CUST_KEY(w_id, d_id, o_id);
  pkey = MAKE_HASH_KEY(ORDER_TID, key);
  struct elem *o_e = hash_insert(p, pkey, sizeof(struct tpcc_order), NULL);
  assert(o_e);

  p->ninserts++;
  o_e->ref_count++;
  struct tpcc_order *o_r = (struct tpcc_order *)o_e->value;
  assert(o_r);

  o_r->o_id = o_id;
  o_r->o_c_id = c_id;
  o_r->o_d_id = d_id;
  o_r->o_w_id = w_id;
  o_r->o_entry_d = o_entry_d;
  o_r->o_ol_cnt = ol_cnt;
  
  // for now only local
  assert(remote == 0);
  o_r->o_all_local = 1;

  /* 
   * EXEC SQL INSERT IN TO NEW_ORDER (no_o_id , no_d_id , no_w _id )
   * VALUES (:o_id , :d _id , :w _id );
   */
  pkey = MAKE_HASH_KEY(NEW_ORDER_TID, key);
  struct elem *no_e = hash_insert(p, pkey, sizeof(struct tpcc_new_order), NULL);
  assert(no_e);

  p->ninserts++;
  no_e->ref_count++;
  struct tpcc_new_order *no_r = (struct tpcc_new_order *)no_e->value;
  assert(no_r);

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
    struct elem *i_e = hash_lookup(item_p, pkey);
    assert(i_e);

    struct tpcc_item *i_r = (struct tpcc_item *) i_e->value;
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
    key = MAKE_STOCK_KEY(w_id, ol_i_id);
    pkey = MAKE_HASH_KEY(STOCK_TID, key);
    struct elem *s_e = hash_lookup(p, pkey);
    assert(s_e);

    struct tpcc_stock *s_r = (struct tpcc_stock *) s_e->value;
    assert(s_r);
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
      assert(0); // for now all local
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
    struct elem *ol_e = hash_insert(p, pkey, sizeof(struct tpcc_order_line), NULL);
    assert(ol_e);

    p->ninserts++;
    no_e->ref_count++;
    struct tpcc_order_line *ol_r = (struct tpcc_order_line *) ol_e->value;
    assert(ol_r);

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
  
  return 0;
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
  struct tpcc_query *q = (struct tpcc_query *) arg;

  //return tpcc_run_neworder_txn(hash_table, s, q); 
  return tpcc_run_neworder_txn_v1(hash_table, s, q); 
}

struct benchmark tpcc_bench = {
  .alloc_query = tpcc_alloc_query,
  .load_data = tpcc_load_data,
  .get_next_query = tpcc_get_next_query,
  .run_txn = tpcc_run_txn,
  .hash_get_server = tpcc_hash_get_server,
};
