#!/bin/bash
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

  for i in {18,36,72,144}; do
    for j in {1,2,3}; do
      echo ./ycsb -s $i -d 0 -b $batchsize -i 100000000 -o 10 -t 1179648 -w $write -a 100 -h 1 -p 16 
      echo ./ycsb -s $i -d 0 -b $batchsize -i 100000000 -o 10 -t 1179648 -w $write -a 100 -h 1 -p 16 >> $outfile
      ./ycsb -s $i -d 0 -b $batchsize -i 100000000 -o 10 -t 1179648 -w $write -a 100 -h 1 -p 16 >> $outfile
    done
  done

}

cd ../..

make clean
make "DFLAGS=-DSHARED_NOTHING -DPTHREAD_SPINLOCK -DHT_ENABLED"
do_test_sn sn-multitable-hyperthread-contention-scaling.log 1 100 100000000
do_test_sn sn-multitable-hyperthread-conflict-scaling.log 1 0 100000000

make clean
make "DFLAGS=-DHT_ENABLED"
do_test_sn tr-nowait-b1-multitable-hyperthread-contention-scaling.log 1 100 100000000
do_test_sn tr-nowait-b1-multitable-hyperthread-conflict-scaling.log 1 0 100000000
do_test_sn tr-nowait-b8-multitable-hyperthread-contention-scaling.log 8 100 100000000
do_test_sn tr-nowait-b8-multitable-hyperthread-conflict-scaling.log 8 0 100000000

#make clean
#make "DFLAGS=-DENABLE_WAIT_DIE_CC -DHT_ENABLED"
#do_test_sn tr-b1-waitdie-multitable-hyperthread-contention-scaling.log 1 100 100000000
#do_test_sn tr-b1-waitdie-multitable-hyperthread-conflict-scaling.log 1 0 100000000
#do_test_sn tr-b8-waitdie-multitable-hyperthread-contention-scaling.log 8 100 100000000
#do_test_sn tr-b8-waitdie-multitable-hyperthread-conflict-scaling.log 8 0 100000000


