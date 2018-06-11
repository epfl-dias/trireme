#!/bin/bash

do_test_se () {
  outfile=$1
  batchsize=$2

  for i in {1,2,4,8,16,32,64}; do
    for j in {1,2}; do
      echo ./ycsb -s 64 -d 0 -b $batchsize -i 1000000 -o 10 -t 64000000 -w `expr 100 - $i` -a 100 -h 16 -p 1 >> $outfile
      ./ycsb -s 64 -d 0 -b $batchsize -i 1000000 -o 10 -t 64000000 -w `expr 100 - $i` -a 100 -h 16 -p 1 >> $outfile
    done
  done
}

do_test_sn () {
  outfile=$1
  batchsize=$2

  for i in {1,2,4,8,16,32,64}; do
    for j in {1,2}; do
      echo ./ycsb -s 64 -d 0 -b $batchsize -i 1000000 -o 10 -t 64000000 -w `expr 100 - $i` -a 100 -h 1 -p 16 >> $outfile
      ./ycsb -s 64 -d 0 -b $batchsize -i 1000000 -o 10 -t 64000000 -w `expr 100 - $i` -a 100 -h 1 -p 16 >> $outfile
    done
  done
}

#make clean
#make "DFLAGS=-DENABLE_WAIT_DIE_CC "
#do_test_sn tr-b1-update-sensitivity.log 1
#do_test_sn tr-b1-update-sensitivity.log 1
#do_test_sn tr-b4-update-sensitivity.log 4

make clean
make "DFLAGS=-DENABLE_WAIT_DIE_CC -DENABLE_OP_BATCHING"
do_test_sn tr-b1-update-sensitivity-with-batching.log 1

make clean
make "DFLAGS=-DSHARED_EVERYTHING -DSE_LATCH -DPTHREAD_SPINLOCK -DENABLE_WAIT_DIE_CC "
do_test_se se-update-sensitivity.log 1

make clean
make "DFLAGS=-DSHARED_NOTHING -DPTHREAD_SPINLOCK"
do_test_sn sn-update-sensitivity.log 1


