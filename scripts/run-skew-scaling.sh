#!/bin/bash
do_test_se () {
  outfile=$1
  batchsize=$2

  for i in {1,2,4,8,16,32,64}; do
    for j in {1,2,3}; do
      echo ./ycsb -s $i -d 0 -b $batchsize -i 10000000 -o 10 -t `expr $i \* 1000000` -w 100 -a 100 -h 16 -p 1 >> $outfile
      ./ycsb -s $i -d 0 -b $batchsize -i 10000000 -o 10 -t `expr $i \* 1000000` -w 100 -a 100 -h 16 -p 1 >> $outfile
    done
  done
}

do_test_sn () {
  outfile=$1
  batchsize=$2

  #16 recs
  for i in {1,2,4,8,16}; do
    for j in {1,2,3}; do
      echo ./ycsb -s $i -d 0 -b $batchsize -i 10000000 -o 10 -t `expr $i \* 1000000` -w 100 -a 100 -h `expr 16 / $i` -p $i >> $outfile
      ./ycsb -s $i -d 0 -b $batchsize -i 10000000 -o 10 -t `expr $i \* 1000000` -w 100 -a 100 -h `expr 16 / $i` -p $i >> $outfile
    done
  done

  #32 and 64 now
  for i in {32,64}; do
    for j in {1,2,3}; do
      echo ./ycsb -s $i -d 0 -b $batchsize -i 10000000 -o 10 -t `expr $i \* 1000000` -w 100 -a 100 -h 1 -p 16 >> $outfile
      ./ycsb -s $i -d 0 -b $batchsize -i 10000000 -o 10 -t `expr $i \* 1000000` -w 100 -a 100 -h 1 -p 16 >> $outfile
    done
  done
}

#make clean
#make "DFLAGS=-DENABLE_WAIT_DIE_CC "
#do_test_sn tr-b1-skew-scaling.log 1
#do_test_sn tr-b2-skew-scaling.log 2
#do_test_sn tr-b4-skew-scaling.log 4
#do_test_sn tr-b8-skew-scaling.log 8
#

make clean
make "DFLAGS=-DENABLE_WAIT_DIE_CC -DENABLE_OP_BATCHING"
do_test_sn tr-b4-skew-scaling-with-batching.log 4


#make clean
#make "DFLAGS=-DSHARED_NOTHING -DPTHREAD_SPINLOCK"
#do_test_sn sn-skew-scaling.log 1


#make clean
#make "DFLAGS=-DSHARED_EVERYTHING -DSE_LATCH -DPTHREAD_SPINLOCK -DENABLE_WAIT_DIE_CC "
#do_test_se se-skew-scaling.log 1


