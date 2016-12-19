#!/bin/bash
do_test () {
  outfile=$1
  batchsize=$2
  w=$3

  for i in {1,2,4,8,18,36,54,72}; do
      for j in {1,2,3}; do
          echo ./ycsb -s $i -d 0 -b $batchsize -i 100000000 -o 10 -t 1179648 -w $w -a 100 -h 16 -p 1

          echo ./ycsb -s $i -d 0 -b $batchsize -i 100000000 -o 10 -t 1179648 -w $w -a 100 -h 16 -p 1 >> $outfile

          ./ycsb -s $i -d 0 -b $batchsize -i 100000000 -o 10 -t 1179648 -w $w -a 100 -h 16 -p 1 >> $outfile

      done
  done
}

cd ../..

make clean
make "DFLAGS=-DSHARED_NOTHING -DPTHREAD_SPINLOCK"
do_test sn-multitable-ronly-load-imbal.log 1 100

make clean
make "DFLAGS=-DENABLE_WAIT_DIE_CC "
do_test tr-b1-multitable-ronly-load-imbal.log 1 100
do_test tr-b8-multitable-ronly-load-imbal.log 8 100

make clean
make
do_test tr-nowait-b1-multitable-ronly-load-imbal.log 1 100
do_test tr-nowait-b8-multitable-ronly-load-imbal.log 8 100
