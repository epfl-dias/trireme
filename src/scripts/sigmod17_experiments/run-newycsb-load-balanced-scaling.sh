#!/bin/bash
do_test () {
  outfile=$1
  batchsize=$2
  w=$3
  iter=$4

  for i in {1,2,4,8,18,36,72}; do
      for j in {1,2,3}; do
          echo ./ycsb -s $i -d 0 -b $batchsize -i $iter -o 10 -r 10 -t 72 -w $w
          echo ./ycsb -s $i -d 0 -b $batchsize -i $iter -o 10 -r 10 -t 72 -w $w >> $outfile
          ./ycsb -s $i -d 0 -b $batchsize -i $iter -o 10 -r 10 -t 72 -w $w >> $outfile
      done
  done
}

cd ../..

make clean
make "DFLAGS=-DSHARED_NOTHING -DPTHREAD_SPINLOCK"
do_test sn-newycsb-ronly-load-balanced.log 1 100 100000000
do_test sn-newycsb-write-load-balanced.log 1 0 100000000

make clean
make "DFLAGS=-DENABLE_WAIT_DIE_CC "
do_test tr-b1-waitdie-newycsb-ronly-load-balanced.log 1 100 100000000
do_test tr-b1-waitdie-newycsb-write-load-balanced.log 1 0 100000000
do_test tr-b8-waitdie-newycsb-ronly-load-balanced.log 8 100 100000000
do_test tr-b8-waitdie-newycsb-write-load-balanced.log 8 0 100000000

make clean
make
do_test tr-nowait-b1-newycsb-ronly-load-balanced.log 1 100 100000000
do_test tr-nowait-b1-newycsb-write-load-balanced.log 1 0 100000000
do_test tr-nowait-b8-newycsb-ronly-load-balanced.log 8 100 100000000
do_test tr-nowait-b8-newycsb-write-load-balanced.log 8 0 100000000

#make clean
#make "DFLAGS=-DENABLE_BWAIT_CC "
#do_test_sn tr-b1-bwait-newycsb-ronly-load-balanced.log 1 100 100000
#do_test_sn tr-b8-bwait-newycsb-ronly-load-balanced.log 8 100 100000
#do_test_sn tr-b1-bwait-newycsb-write-load-balanced.log 1 0 100000
#do_test_sn tr-b8-bwait-newycsb-write-load-balanced.log 8 0 100000

#make clean
#make "DFLAGS=-DENABLE_WAIT_DIE_CC -DENABLE_OP_BATCHING"
#do_test_sn tr-b4-skew-scaling-with-batching.log 4


