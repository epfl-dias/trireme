#!/bin/bash
do_test () {
  outfile=$1
  batchsize=$2
  w=$3

  for i in {1,2,4,8,18,36,54,72}; do
      for j in {1,2,3}; do
          if [ $i -eq 1 ]; then
              echo ./ycsb -s $i -d 0 -b $batchsize -i 100000000 -o 10 -r 10 -t `expr $i \* 72` -w $w
              echo ./ycsb -s $i -d 0 -b $batchsize -i 100000000 -o 10 -r 10 -t `expr $i \* 72` -w $w >> $outfile
              ./ycsb -s $i -d 0 -b $batchsize -i 100000000 -o 10 -r 10 -t `expr $i \* 72` -w $w >> $outfile
          else
              echo ./ycsb -s $i -d 0 -b $batchsize -i 100000000 -o 10 -r 10 -t `expr $i \* 72` -w $w -a 100 -h 72 -p 1
              echo ./ycsb -s $i -d 0 -b $batchsize -i 100000000 -o 10 -r 10 -t `expr $i \* 72` -w $w -a 100 -h 72 -p 1 >> $outfile
              ./ycsb -s $i -d 0 -b $batchsize -i 100000000 -o 10 -r 10 -t `expr $i \* 72` -w $w -a 100 -h 72 -p 1 >> $outfile

          fi
      done
  done
}

cd ../..

make clean
make "DFLAGS=-DSHARED_NOTHING -DPTHREAD_SPINLOCK"
do_test sn-newycsb-ronly-load-imbal.log 1 100
do_test sn-newycsb-write-load-imbal.log 1 0

make clean
make "DFLAGS=-DENABLE_WAIT_DIE_CC "
do_test tr-b1-newycsb-ronly-load-imbal.log 1 100
do_test tr-b1-newycsb-write-load-imbal.log 1 0
do_test tr-b8-newycsb-ronly-load-imbal.log 8 100
do_test tr-b8-newycsb-write-load-imbal.log 8 0

make clean
make
do_test tr-nowait-b1-newycsb-ronly-load-imbal.log 1 100
do_test tr-nowait-b1-newycsb-write-load-imbal.log 1 0
do_test tr-nowait-b8-newycsb-ronly-load-imbal.log 8 100
do_test tr-nowait-b8-newycsb-write-load-imbal.log 8 0
