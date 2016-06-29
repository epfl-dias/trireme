#!/bin/bash

do_test_se () {
  outfile=$1
  batchsize=$2

  for i in {16,32,64,128,256,512,1024,2048,4096}; do
    for j in {1,2,3}; do
      echo ./ycsb -s 64 -d 0 -b $batchsize -i 1000000 -o 10 -t 64000000 -w 0 -a 100 -h $i -p 1 >> $outfile
      ./ycsb -s 64 -d 0 -b $batchsize -i 1000000 -o 10 -t 64000000 -w 0 -a 100 -h $i -p 1 >> $outfile
    done
  done

}

do_test_sn () {
  outfile=$1
  batchsize=$2

  #16 recs
  for j in {1,2,3}; do
    echo ./ycsb -s 64 -d 0 -b $batchsize -i 1000000 -o 10 -t 64000000 -w 0 -a 100 -h 1 -p 16 >> $outfile
    ./ycsb -s 64 -d 0 -b $batchsize -i 1000000 -o 10 -t 64000000 -w 0 -a 100 -h 1 -p 16 >> $outfile
  done

  #32 recs
  for j in {1,2,3}; do
    echo ./ycsb -s 64 -d 0 -b $batchsize -i 1000000 -o 10 -t 64000000 -w 0 -a 100 -h 1 -p 32 >> $outfile
    ./ycsb -s 64 -d 0 -b $batchsize -i 1000000 -o 10 -t 64000000 -w 0 -a 100 -h 1 -p 32 >> $outfile
  done

  #rest
  for i in {1,2,4,8,16,32,64}; do
    for j in {1,2,3}; do
      echo ./ycsb -s 64 -d 0 -b $batchsize -i 10000000 -o 10 -t 64000000 -w 0 -a 100 -h $i -p 64 >> $outfile
      ./ycsb -s 64 -d 0 -b $batchsize -i 10000000 -o 10 -t 64000000 -w 0 -a 100 -h $i -p 64 >> $outfile
    done
  done
}

#make clean
#make "DFLAGS=-DENABLE_WAIT_DIE_CC "
#do_test_sn tr-b1-contention-sensitivity.log 1
#do_test_sn tr-b2-contention-sensitivity.log 2
#do_test_sn tr-b4-contention-sensitivity.log 4
#do_test_sn tr-b8-contention-sensitivity.log 8

make clean
make "DFLAGS=-DENABLE_WAIT_DIE_CC -DENABLE_OP_BATCHING"
do_test_sn tr-b1-contention-sensitivity-with-batching.log 1

#make clean
#make "DFLAGS=-DSHARED_NOTHING -DPTHREAD_SPINLOCK"
#do_test_sn sn-contention-sensitivity.log 1


#make clean
#make "DFLAGS=-DSHARED_EVERYTHING -DSE_LATCH -DPTHREAD_SPINLOCK -DENABLE_WAIT_DIE_CC "
#do_test_se se-contention-sensitivity.log 1


