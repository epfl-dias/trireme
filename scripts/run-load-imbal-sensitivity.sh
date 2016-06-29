#!/bin/bash

do_test () {
  outfile=$1
  batchsize=$2

  for i in {1,2,4,8,16}; do
    for j in {1,2,3}; do
      echo ./ycsb -s 64 -d 0 -b $batchsize -i 10000000 -o 10 -t 64000000 -w 100 -a 100 -h $i -p `expr 16 / $i` >> $outfile
      ./ycsb -s 64 -d 0 -b $batchsize -i 10000000 -o 10 -t 64000000 -w 100 -a 100 -h $i -p `expr 16 / $i` >> $outfile
    done
  done
}

make clean
make "DFLAGS=-DENABLE_WAIT_DIE_CC "
#do_test tr-b1-load-imbal-sensitivity.log 1
#do_test tr-b2-load-imbal-sensitivity.log 2
do_test tr-b4-load-imbal-read-sensitivity.log 4
#do_test tr-b8-load-imbal-sensitivity.log 8

#make clean
#make "DFLAGS=-DENABLE_WAIT_DIE_CC -DENABLE_OP_BATCHING"
#do_test tr-b1-load-imbal-sensitivity-with-batching.log 1

#make clean
#make "DFLAGS=-DSHARED_NOTHING -DPTHREAD_SPINLOCK"
#do_test sn-load-imbal-sensitivity.log 1


#make clean
#ake "DFLAGS=-DSHARED_EVERYTHING -DSE_LATCH -DPTHREAD_SPINLOCK -DENABLE_WAIT_DIE_CC "
#o_test se-load-imbal-sensitivity.log 1


