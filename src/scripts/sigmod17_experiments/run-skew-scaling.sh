#!/bin/bash
do_test_se () {
  outfile=$1
  batchsize=$2
  write=$3

  for i in {1,2,4,8,18,36,54,72}; do
    for j in {1,2,3}; do
      echo ./ycsb -s $i -d 0 -b $batchsize -i 100000000 -o 10 -t 1179648 -w $write -a 100 -h 16 -p 1
      echo ./ycsb -s $i -d 0 -b $batchsize -i 100000000 -o 10 -t 1179648 -w $write -a 100 -h 16 -p 1 >> $outfile
      ./ycsb -s $i -d 0 -b $batchsize -i 100000000 -o 10 -t 1179648 -w $write -a 100 -h 16 -p 1 >> $outfile
    done
  done
}

do_test_sn () {
  outfile=$1
  batchsize=$2
  write=$3

  for j in {1,2,3}; do
      echo ./ycsb -s 1 -d 0 -b $batchsize -i 100000000 -o 10 -t 1179648 -w $write -a 100 -h 16 -p 1
      echo ./ycsb -s 1 -d 0 -b $batchsize -i 100000000 -o 10 -t 1179648 -w $write -a 100 -h 16 -p 1 >> $outfile
      ./ycsb -s 1 -d 0 -b $batchsize -i 100000000 -o 10 -t 1179648 -w $write -a 100 -h 16 -p 1 >> $outfile
  done

  for j in {1,2,3}; do
      echo ./ycsb -s 2 -d 0 -b $batchsize -i 100000000 -o 10 -t 1179648 -w $write -a 100 -h 8 -p 2
      echo ./ycsb -s 2 -d 0 -b $batchsize -i 100000000 -o 10 -t 1179648 -w $write -a 100 -h 8 -p 2 >> $outfile
      ./ycsb -s 2 -d 0 -b $batchsize -i 100000000 -o 10 -t 1179648 -w $write -a 100 -h 8 -p 2 >> $outfile
  done

  for j in {1,2,3}; do
      echo ./ycsb -s 4 -d 0 -b $batchsize -i 100000000 -o 10 -t 1179648 -w $write -a 100 -h 4 -p 4
      echo ./ycsb -s 4 -d 0 -b $batchsize -i 100000000 -o 10 -t 1179648 -w $write -a 100 -h 4 -p 4 >> $outfile
      ./ycsb -s 4 -d 0 -b $batchsize -i 100000000 -o 10 -t 1179648 -w $write -a 100 -h 4 -p 4 >> $outfile
  done

  for j in {1,2,3}; do
      echo ./ycsb -s 8 -d 0 -b $batchsize -i 100000000 -o 10 -t 1179648 -w $write -a 100 -h 2 -p 8
      echo ./ycsb -s 8 -d 0 -b $batchsize -i 100000000 -o 10 -t 1179648 -w $write -a 100 -h 2 -p 8 >> $outfile
      ./ycsb -s 8 -d 0 -b $batchsize -i 100000000 -o 10 -t 1179648 -w $write -a 100 -h 2 -p 8 >> $outfile
  done

  for j in {1,2,3}; do
      echo ./ycsb -s 18 -d 0 -b $batchsize -i 100000000 -o 10 -t 1179648 -w $write -a 100 -h 1 -p 16
      echo ./ycsb -s 18 -d 0 -b $batchsize -i 100000000 -o 10 -t 1179648 -w $write -a 100 -h 1 -p 16 >> $outfile
      ./ycsb -s 18 -d 0 -b $batchsize -i 100000000 -o 10 -t 1179648 -w $write -a 100 -h 1 -p 16 >> $outfile
  done

  for i in {18,36,54,72}; do
    for j in {1,2,3}; do
      echo ./ycsb -s $i -d 0 -b $batchsize -i 100000000 -o 10 -t 1179648 -w $write -a 100 -h 1 -p 16 
      echo ./ycsb -s $i -d 0 -b $batchsize -i 100000000 -o 10 -t 1179648 -w $write -a 100 -h 1 -p 16 >> $outfile
      ./ycsb -s $i -d 0 -b $batchsize -i 100000000 -o 10 -t 1179648 -w $write -a 100 -h 1 -p 16 >> $outfile
    done
  done

}

do_socket_local () {
  outfile=$1
  batchsize=$2
  write=$3

  for k in {1,2,3}; do
      echo ./ycsb -s 36 -d 0 -b $batchsize -i 100000000 -o 10 -t 1179648 -w $write -a 100 -h 16 -p 1 
      echo ./ycsb -s 36 -d 0 -b $batchsize -i 100000000 -o 10 -t 1179648 -w $write -a 100 -h 16 -p 1 >> $outfile
      ./ycsb -s 36 -d 0 -b $batchsize -i 100000000 -o 10 -t 1179648 -w $write -a 100 -h 16 -p 1 >> $outfile

  done
}
cd ../..

#no latch
#make clean
#make "DFLAGS=-DSHARED_EVERYTHING"
#do_test_se se-multitable-nowait-nolatch-ronly-skew-scaling.log 1 100

#waitdie spinlock
#make clean
#make "DFLAGS=-DSHARED_EVERYTHING -DSE_LATCH -DPTHREAD_SPINLOCK -DENABLE_WAIT_DIE_CC"
#do_test_se se-multitable-waitdie-ronly-skew-scaling.log 1 100
#do_test_se se-multitable-waitdie-wonly-conflict-scaling.log 1 0

#nowait se ownerlist
#make clean
#make "DFLAGS=-DSHARED_EVERYTHING -DSE_LATCH -DPTHREAD_SPINLOCK -DENABLE_NOWAIT_OWNER_CC"
#do_test_se se-multitable-nowait-ownerlist-with-spinlock-ronly-skew-scaling.log 1 100
#do_test_se se-multitable-nowait-ownerlist-with-spinlock-wonly-conflict-scaling.log 1 0

#nowait rwlock
#make clean
#make "DFLAGS=-DSHARED_EVERYTHING -DSE_LATCH -DRW_LOCK -DCUSTOM_RW_LOCK"
#do_test_se se-multitable-nowait-rwlock-ronly-skew-scaling.log 1 100
#do_test_se se-multitable-nowait-rwlock-wonly-conflict-scaling.log 1 0

#nowait pthrwlock
#make clean
#make "DFLAGS=-DSHARED_EVERYTHING -DSE_LATCH -DRW_LOCK"
#do_test_se se-multitable-nowait-pthread-rwlock-ronly-skew-scaling.log 1 100
#do_test_se se-multitable-nowait-pthread-rwlock-wonly-conflict-scaling.log 1 0

#nowait drwlock
#make clean
#make "DFLAGS=-DSHARED_EVERYTHING -DSE_LATCH -DDRW_LOCK"
#do_test_se se-multitable-nowait-custom-drwlock-ronly-skew-scaling.log 1 100
#do_test_se se-multitable-nowait-custom-drwlock-wonly-conflict-scaling.log 1 0

#waitdie msg
make clean
make "DFLAGS=-DENABLE_WAIT_DIE_CC "
#do_test_sn tr-waitdie-b1-multitable-ronly-skew-scaling.log 1 100
do_test_sn tr-waitdie-b1-multitable-ronly-skew-scaling.log 4 100
#do_test_sn tr-waidie-b8-multitable-ronly-skew-scaling.log 8 100
#do_test_sn tr-waitdie-b1-multitable-write-skew-scaling.log 1 0
#do_test_sn tr-waitdie-b8-multitable-write-skew-scaling.log 8 0

#nowait msg
#make clean
#make
#do_test_sn tr-nowait-b1-multitable-ronly-skew-scaling.log 1 100
#do_test_sn tr-nowait-b8-multitable-ronly-skew-scaling.log 8 100
#do_test_sn tr-nowait-b1-multitable-write-skew-scaling.log 1 0
#do_test_sn tr-nowait-b8-multitable-write-skew-scaling.log 8 0

#sn
#make clean
#make "DFLAGS=-DSHARED_NOTHING -DPTHREAD_SPINLOCK"
#do_test_sn sn-multitable-ronly-skew-scaling.log 1 100
#do_test_sn sn-multitable-write-skew-scaling.log 1 0

#make clean
#make "DFLAGS=-DENABLE_BWAIT_CC"
#do_test_sn tr-b2-bwait-skew-scaling.log 1
#do_test_sn tr-b2-skew-scaling.log 2
#do_test_sn tr-b4-skew-scaling.log 4
#do_test_sn tr-b8-bwait-skew-scaling.log 8

#make clean
#make "DFLAGS=-DENABLE_WAIT_DIE_CC -DENABLE_OP_BATCHING"
#do_test_sn tr-b4-skew-scaling-with-batching.log 4



