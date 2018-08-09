#include <limits.h>
#include "headers.h"
#include "smphashtable.h"
#include "partition.h"
#include "benchmark.h"
#include "plmalloc.h"
#include "tpcc.h"

void init_tpcc_seq_array(){

  int total = 0;
  for (int i = 0 ; i < NO_MIX ; ++i){
    sequence[i] = 1;
  }
  total = NO_MIX;
  for(int i = 0 ; i < P_MIX ; ++i){
    sequence[i + total] = 2;
  }
  total = total + P_MIX;
  for(int i = 0 ; i < OS_MIX ; ++i){
    sequence[i+total] = 3;
  }
  total = total + OS_MIX;
  for( int i = 0 ; i < D_MIX ; ++i){
    sequence[i + total] = 4;
  }
  total = total + D_MIX;
  for(int i = 0 ; i < SL_MIX ; ++i){
    sequence[ i + total ] = 5;
  }
  //shuffle elements of the sequence array
  srand ( time(NULL) );
  for (int i = MIX_COUNT-1 ; i > 0; i--)
  {
      int j = rand() % (i+1);
      int temp = sequence[i];
      sequence[i] = sequence[j];
      sequence[j] = temp;
  }
}
void print_tpcc_mix(int g_nservers, struct hash_table *hash_table){
  for (int s = 0; s < g_nservers; s++){
    printf("server %d called new order %d times\n",s,hash_table->partitions[s].new_order_counter);
    printf("server %d called payment %d times\n",s,hash_table->partitions[s].payment_counter);
    printf("server %d called order status %d times\n",s,hash_table->partitions[s].order_status_counter);
    printf("server %d called delivery %d times\n",s,hash_table->partitions[s].delivery_counter);
    printf("server %d called stock level %d times\n",s,hash_table->partitions[s].stock_level_counter);
  }
}

void count_tpcc_transaction(struct hash_table *hash_table, int s, int tnx){
  switch (tnx) {
    case 1:
      ++hash_table->partitions[s].new_order_counter;
      break;
    case 2:
      ++hash_table->partitions[s].payment_counter;
      break;
    case 3:
      ++hash_table->partitions[s].order_status_counter;
      break;
    case 4:
      ++hash_table->partitions[s].delivery_counter;
      break;
    case 5:
      ++hash_table->partitions[s].stock_level_counter;
      break;
  }
}
