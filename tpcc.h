#ifndef __TPCC_H_
#define __TPCC_H_

#define TID_MASK 0x0F00000000000000
#define MAKE_HASH_KEY(tid,rid)  (tid | rid)
#define GET_TID(key) (key & TID_MASK)

#define MAKE_DIST_KEY(w,d) (w * TPCC_NDIST_PER_WH + d)
#define MAKE_CUST_KEY(w,d,c) (MAKE_DIST_KEY(w,d) * TPCC_NCUST_PER_DIST + c)
#define MAKE_OL_KEY(w,d,o,ol) (MAKE_CUST_KEY(w,d,o) * TPCC_MAX_OL_PER_ORDER + ol)

#define TPCC_MAX_ITEMS 100000
#define TPCC_NDIST_PER_WH 10
#define TPCC_NCUST_PER_DIST 3000
#define TPCC_MAX_OL_PER_ORDER 15

#define FIRST_NAME_MIN_LEN 8
#define FIRST_NAME_LEN 16
#define LAST_NAME_LEN 16

#define WAREHOUSE_TID 0x0100000000000000

struct tpcc_warehouse {
	int64_t w_id;
	char w_name[10];
	char w_street[2][20];
	char w_city[20];
	char w_state[2];
	char w_zip[9];
	double w_tax;
	double w_ytd;
};

#define DISTRICT_TID 0x0200000000000000

struct tpcc_district {
	int64_t d_id;
	int64_t d_w_id;
	char d_name[10];
	char d_street[2][20];
	char d_city[20];
	char d_state[2];
	char d_zip[9];
	double d_tax;
	double d_ytd;
	int64_t d_next_o_id;
};

#define CUSTOMER_TID 0x0300000000000000

struct tpcc_customer {
	int64_t c_id;
	int64_t c_d_id;
	int64_t c_w_id;
	char c_first[FIRST_NAME_LEN];
	char c_middle[2];
	char c_last[LAST_NAME_LEN];
	char c_street[2][20];
	char c_city[20];
	char c_state[2];
	char c_zip[9];
	char c_phone[16];
	int64_t c_since;
	char c_credit[2];
	int64_t c_credit_lim;
	int64_t c_discount;
	double c_balance;
	double c_ytd_payment;
	uint64_t c_payment_cnt;
	uint64_t c_delivery_cnt;
	char c_data[500];
};

#define HISTORY_TID 0x0400000000000000

struct tpcc_history {
	int64_t h_c_id;
	int64_t h_c_d_id;
	int64_t h_c_w_id;
	int64_t h_d_id;
	int64_t h_w_id;
	int64_t h_date;
	double h_amount;
	char h_data[24];
};

#define NEW_ORDER_TID 0x0500000000000000

struct tpcc_new_order {
	int64_t no_o_id;
	int64_t no_d_id;
	int64_t no_w_id;
};

#define ORDER_TID 0x0600000000000000

struct tpcc_order {
	int64_t o_id;
	int64_t o_c_id;
	int64_t o_d_id;
	int64_t o_w_id;
	int64_t o_entry_d;
	int64_t o_carrier_id;
	int64_t o_ol_cnt;
	int64_t o_all_local;
};

#define ORDER_LINE_TID 0x0700000000000000

struct tpcc_order_line {
	int64_t ol_o_id;
	int64_t ol_d_id;
	int64_t ol_w_id;
	int64_t ol_number;
	int64_t ol_i_id;
	int64_t ol_supply_w_id;
	int64_t ol_delivery_d;
	int64_t ol_quantity;
	double ol_amount;
	char ol_dist_info[24];
};

#define ITEM_TID 0x0800000000000000

struct tpcc_item {
	int64_t i_id;
	int64_t i_im_id;
	char i_name[24];
	int64_t i_price;
	char i_data[50];
};

#define STOCK_TID 0x0900000000000000

struct tpcc_stock {
	int64_t s_i_id;
	int64_t s_w_id;
	int64_t s_quantity;
	char s_dist[10][24];
	int64_t s_ytd;
	int64_t s_order_cnt;
	int64_t s_remote_cnt;
	char s_data[50];
};

#if 0
struct secondary_record {
  char sr_last_name[LAST_NAME_LEN];
  hash_key sr_rid;
};
#endif

void tpcc_load_data(int id, struct partition *p, struct partition *g);

#endif