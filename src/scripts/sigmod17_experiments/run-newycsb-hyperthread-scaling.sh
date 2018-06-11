#!/bin/bash
do_test_se () {
  outfile=$1
  batchsize=$2
  w=$3
  iter=$4

  for i in {1,2,4,8,18,36,72,144}; do
      for j in {1,2,3}; do
          echo ./ycsb -s $i -d 0 -b $batchsize -i $iter -o 10 -t 72 -w $w
          echo ./ycsb -s $i -d 0 -b $batchsize -i $iter -o 10 -t 72 -w $w >> $outfile
          ./ycsb -s $i -d 0 -b $batchsize -i $iter -o 10 -t 72 -w $w >> $outfile
      done
  done
}

do_test_sn () {
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

  #144 is a special case
  for j in {1,2,3}; do
      echo ./ycsb -s 144 -d 0 -b $batchsize -i $iter -o 10 -r 10 -t 144 -w $w -a 100 -h 1 -p 72
      echo ./ycsb -s 144 -d 0 -b $batchsize -i $iter -o 10 -r 10 -t 144 -w $w -a 100 -h 1 -p 72 >> $outfile
      ./ycsb -s 144 -d 0 -b $batchsize -i $iter -o 10 -r 10 -t 144 -w $w -a 100 -h 1 -p 72 >> $outfile
  done

}

cd ../..

make clean
make "DFLAGS=-DSHARED_EVERYTHING -DSE_LATCH -DPTHREAD_SPINLOCK -DENABLE_WAIT_DIE_CC -DHT_ENABLED"
#do_test_se se-newycsb-waitdie-spinlock-hyperthread-scaling.log 1 100 100000000
do_test_se se-newycsb-waitdie-spinlock-hyperthread-conflict-scaling.log 1 0 100000000
#
make clean
make "DFLAGS=-DSHARED_EVERYTHING -DSE_LATCH -DPTHREAD_SPINLOCK -DENABLE_NOWAIT_OWNER_CC -DHT_ENABLED"
#do_test_se se-newycsb-nowait-with-ownerlist-spinlock-hyperthread-contention-scaling.log 1 100 100000000
do_test_se se-newycsb-nowait-with-ownerlist-spinlock-hyperthread-conflict-scaling.log 1 0 100000000
#
make clean
make "DFLAGS=-DSHARED_EVERYTHING -DSE_LATCH -DRW_LOCK -DHT_ENABLED"
#do_test_se se-nowait-newycsb-pthread-rwlock-hyperthread-contention-scaling.log 1 100 100000000
do_test_se se-nowait-newycsb-pthread-rwlock-hyperthread-conflict-scaling.log 1 0 100000000
#
make clean
make "DFLAGS=-DSHARED_EVERYTHING -DSE_LATCH -DDRW_LOCK -DHT_ENABLED"
#do_test_se se-nowait-newycsb-custom-drwlock-hyperthread-contention-scaling.log 1 100 100000000
do_test_se se-nowait-newycsb-custom-drwlock-hyperthread-conflict-scaling.log 1 0 100000000

#make clean
#make "DFLAGS=-DSHARED_NOTHING -DPTHREAD_SPINLOCK -DHT_ENABLED"
#do_test_sn sn-newycsb-hyperthread-contention-scaling.log 1 100 100000000
#do_test_sn sn-newycsb-hyperthread-conflict-scaling.log 1 0 100000000

#make clean
#make "DFLAGS=-DENABLE_WAIT_DIE_CC -DHT_ENABLED"
#do_test_sn tr-b1-waitdie-newycsb-hyperthread-contention-scaling.log 1 100 100000000
#do_test_sn tr-b1-waitdie-newycsb-hyperthread-conflict-scaling.log 1 0 100000000
#do_test_sn tr-b8-waitdie-newycsb-hyperthread-contention-scaling.log 8 100 100000000
#do_test_sn tr-b8-waitdie-newycsb-hyperthread-conflict-scaling.log 8 0 100000000

#make clean
#make "DFLAGS=-DHT_ENABLED"
#do_test_sn tr-nowait-b1-newycsb-hyperthread-contention-scaling.log 1 100 100000000
#do_test_sn tr-nowait-b1-newycsb-hyperthread-conflict-scaling.log 1 0 100000000
#do_test_sn tr-nowait-b8-newycsb-hyperthread-contention-scaling.log 8 100 100000000
#do_test_sn tr-nowait-b8-newycsb-hyperthread-conflict-scaling.log 8 0 100000000


