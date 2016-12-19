#!/bin/bash
do_test_se () {
  outfile=$1
  batchsize=$2
  w=$3
  iter=$4

  for i in {1,2,4,8,18,36,54,72}; do
      for j in {1,2,3}; do
          echo ./ycsb -s $i -d 0 -b $batchsize -i $iter -o 10 -t 72 -w $w
          echo ./ycsb -s $i -d 0 -b $batchsize -i $iter -o 10 -t 72 -w $w >> $outfile
          ./ycsb -s $i -d 0 -b $batchsize -i $iter -o 10 -t 72 -w $w >> $outfile
      done
  done
}

cd ../..

make clean
make "DFLAGS=-DSHARED_EVERYTHING -DENABLE_WAIT_DIE_CC "
do_test_se se-newycsb-waitdie-nolatch-contention-scaling.log 1 100 100000000

make clean
make "DFLAGS=-DSHARED_EVERYTHING"
do_test_se se-newycsb-nowait-nolatch-contention-scaling.log 1 100 100000000

#make clean
#make "DFLAGS=-DSHARED_EVERYTHING -DSE_LATCH -DMUTEX_LOCK -DENABLE_WAIT_DIE_CC "
#do_test_se se-newycsb-waitdie-mutexlock-scaling.log 1 100 100000000
#
#make clean
#make "DFLAGS=-DSHARED_EVERYTHING -DSE_LATCH -DMUTEX_LOCK"
#do_test_se se-nowait-newycsb-mutexlock-scaling.log 1 100 100000000
#
#make clean
#make "DFLAGS=-DSHARED_EVERYTHING -DSE_LATCH -DTAS_LOCK -DENABLE_WAIT_DIE_CC "
#do_test_se se-newycsb-waitdie-taslock-scaling.log 1 100 100000000
#
#make clean
#make "DFLAGS=-DSHARED_EVERYTHING -DSE_LATCH -DTAS_LOCK"
#do_test_se se-nowait-newycsb-taslock-scaling.log 1 100 100000000
#
make clean
make "DFLAGS=-DSHARED_EVERYTHING -DSE_LATCH -DPTHREAD_SPINLOCK -DENABLE_WAIT_DIE_CC "
do_test_se se-newycsb-waitdie-spinlock-contention-scaling.log 1 100 100000000
do_test_se se-newycsb-waitdie-spinlock-conflict-scaling.log 1 0 100000000

make clean
make "DFLAGS=-DSHARED_EVERYTHING -DSE_LATCH -DPTHREAD_SPINLOCK -DENABLE_NOWAIT_OWNER_CC"
do_test_se se-newycsb-nowait-with-ownerlist-spinlock-contention-scaling.log 1 100 100000000
do_test_se se-newycsb-nowait-with-ownerlist-spinlock-conflict-scaling.log 1 0 100000000

#make clean
#make "DFLAGS=-DSHARED_EVERYTHING -DSE_LATCH -DPTHREAD_SPINLOCK"
#do_test_se se-nowait-newycsb-spinlock-contention-scaling.log 1 100 100000000
#do_test_se se-nowait-newycsb-spinlock-conflict-scaling.log 1 0 100000000

#make clean
#make "DFLAGS=-DSHARED_EVERYTHING -DSE_LATCH -DTICKET_LOCK -DENABLE_WAIT_DIE_CC "
#do_test_se se-newycsb-waitdie-ticketlock-scaling.log 1 100 100000000
#
#make clean
#make "DFLAGS=-DSHARED_EVERYTHING -DSE_LATCH -DTICKET_LOCK"
#do_test_se se-nowait-newycsb-ticketlock-scaling.log 1 100 100000000
#
#make clean
#make "DFLAGS=-DSHARED_EVERYTHING -DSE_LATCH -DHTLOCK -DENABLE_WAIT_DIE_CC "
#do_test_se se-newycsb-waitdie-htlocklock-scaling.log 1 100 100000000
#
#make clean
#make "DFLAGS=-DSHARED_EVERYTHING -DSE_LATCH -DHTLOCK"
#do_test_se se-nowait-newycsb-htlocklock-scaling.log 1 100 100000000
#
#make clean
#make "DFLAGS=-DSHARED_EVERYTHING -DSE_LATCH -DRW_LOCK -DCUSTOM_RW_LOCK"
#do_test_se se-nowait-newycsb-custom-rwlock-contention-scaling.log 1 100 100000000
#do_test_se se-nowait-newycsb-custom-rwlock-conflict-scaling.log 1 0 100000000

make clean
make "DFLAGS=-DSHARED_EVERYTHING -DSE_LATCH -DRW_LOCK"
do_test_se se-nowait-newycsb-pthread-rwlock-contention-scaling.log 1 100 100000000
do_test_se se-nowait-newycsb-pthread-rwlock-conflict-scaling.log 1 0 100000000

make clean
make "DFLAGS=-DSHARED_EVERYTHING -DSE_LATCH -DDRW_LOCK"
do_test_se se-nowait-newycsb-custom-drwlock-contention-scaling.log 1 100 100000000
do_test_se se-nowait-newycsb-custom-drwlock-conflict-scaling.log 1 0 100000000


#make clean
#make "DFLAGS=-DENABLE_BWAIT_CC "
#do_test_sn tr-b1-bwait-newycsb-ronly-load-balanced.log 1 100 100000
#do_test_sn tr-b8-bwait-newycsb-ronly-load-balanced.log 8 100 100000
#do_test_sn tr-b1-bwait-newycsb-write-load-balanced.log 1 0 100000
#do_test_sn tr-b8-bwait-newycsb-write-load-balanced.log 8 0 100000
#


#make clean
#make "DFLAGS=-DENABLE_WAIT_DIE_CC -DENABLE_OP_BATCHING"
#do_test_sn tr-b4-skew-scaling-with-batching.log 4


