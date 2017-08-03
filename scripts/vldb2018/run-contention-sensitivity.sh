#!/bin/bash

do_test_zipf () {
  outfile=$1
  batchsize=$2
  ronly=$3

  if [ $ronly -eq 100 ]; then
      write=0
  else
      write=72
  fi

  for i in {0,0.1,0.2,0.4,0.8,0.9,0.99}; do
    for j in {1,2,3,4,5}; do
      echo ./ycsb -s 72 -d 0 -b $batchsize -o 16 -t 20000000 -w $write -a $i
      echo ./ycsb -s 72 -d 0 -b $batchsize -o 16 -t 20000000 -w $write -a $i >> $outfile
      ./ycsb -s 72 -d 0 -b $batchsize -o 16 -t 20000000 -w $write -a $i >> $outfile
    done
  done

}

do_test_se () {
  outfile=$1
  batchsize=$2
  ronly=$3

  if [ $ronly -eq 100 ]; then
      write=0
  else
      write=72
  fi

  for i in {9,18,36,72,144,288,576,1152,2304,4608,9216,18432}; do
#    for j in {1,2,3,4,5}; do
      echo ./ycsb -s 72 -d 0 -b $batchsize -o 16 -t 100000000 -w $write -a 100 -h $i -p 1
      echo ./ycsb -s 72 -d 0 -b $batchsize -o 16 -t 100000000 -w $write -a 100 -h $i -p 1 >> $outfile
      ./ycsb -s 72 -d 0 -b $batchsize -o 16 -t 100000000 -w $write -a 100 -h $i -p 1 >> $outfile
#    done
  done

}

do_test_sn () {
  outfile=$1
  batchsize=$2
  write=$3

  #9,18 recs
  for i in {9,18}; do 
      for j in {1,2,3,4,5}; do
          echo ./ycsb -s 72 -d 0 -b $batchsize -i 1000000 -o 16 -t 100000000 -w $write-a 100 -h 1 -p $i
          echo ./ycsb -s 72 -d 0 -b $batchsize -i 1000000 -o 16 -t 100000000 -w $write-a 100 -h 1 -p $i >> $outfile
          ./ycsb -s 72 -d 0 -b $batchsize -i 1000000 -o 16 -t 100000000 -w $write -a 100 -h 1 -p $i >> $outfile
      done
  done

  #36 recs
  for j in {1,2,3,4,5}; do
    echo ./ycsb -s 72 -d 0 -b $batchsize -i 1000000 -o 16 -t 100000000 -w $write -a 100 -h 1 -p 36
    echo ./ycsb -s 72 -d 0 -b $batchsize -i 1000000 -o 16 -t 100000000 -w $write -a 100 -h 1 -p 36 >> $outfile
    ./ycsb -s 72 -d 0 -b $batchsize -i 1000000 -o 16 -t 100000000 -w $write -a 100 -h 1 -p 36 >> $outfile
  done

  #rest
  for i in {1,2,4,8,16,32,64,128,256}; do
    for j in {1,2,3,4,5}; do
      echo ./ycsb -s 72 -d 0 -b $batchsize -i 10000000 -o 16 -t 100000000 -w $write -a 100 -h $i -p 72
      echo ./ycsb -s 72 -d 0 -b $batchsize -i 10000000 -o 16 -t 100000000 -w $write -a 100 -h $i -p 72 >> $outfile
      ./ycsb -s 72 -d 0 -b $batchsize -i 10000000 -o 16 -t 100000000 -w $write -a 100 -h $i -p 72 >> $outfile
    done
  done
}

do_test_dl_detect () {
  outfile=$1
  batchsize=$2
  write=$3

  #9,18 recs
  for i in {9,18}; do 
      for j in {1,2,3,4,5}; do
          echo ./ycsb -s 72 -d 0 -b $batchsize -i 1000000 -o 16 -t 100000000 -w $write-a 100 -h 1 -p $i
          echo ./ycsb -s 72 -d 0 -b $batchsize -i 1000000 -o 16 -t 100000000 -w $write-a 100 -h 1 -p $i >> $outfile
          ./ycsb -s 72 -d 0 -b $batchsize -i 1000000 -o 16 -t 100000000 -w $write -a 100 -h 1 -p $i >> $outfile
      done
  done

  #36 recs
  for j in {1,2,3,4,5}; do
    echo ./ycsb -s 72 -d 0 -b $batchsize -i 1000000 -o 16 -t 100000000 -w $write -a 100 -h 1 -p 36
    echo ./ycsb -s 72 -d 0 -b $batchsize -i 1000000 -o 16 -t 100000000 -w $write -a 100 -h 1 -p 36 >> $outfile
    ./ycsb -s 72 -d 0 -b $batchsize -i 1000000 -o 16 -t 100000000 -w $write -a 100 -h 1 -p 36 >> $outfile
  done

  #rest
  for i in {1,2,4,8,16}; do
    for j in {1,2,3,4,5}; do
      echo ./ycsb -s 72 -d 0 -b $batchsize -i 10000000 -o 16 -t 100000000 -w $write -a 100 -h $i -p 71
      echo ./ycsb -s 72 -d 0 -b $batchsize -i 10000000 -o 16 -t 100000000 -w $write -a 100 -h $i -p 71 >> $outfile
      ./ycsb -s 72 -d 0 -b $batchsize -i 10000000 -o 16 -t 100000000 -w $write -a 100 -h $i -p 71 >> $outfile
    done
  done
}

cd ../..

DBGFLAG="-DYCSB_BENCHMARK"

###############################################################################
#                               latching test                                 #
###############################################################################
#silo spinlock
make clean
make "DFLAGS=-DSHARED_EVERYTHING -DSE_LATCH -DPTHREAD_SPINLOCK -DENABLE_SILO_CC $DBGFLAG"
#do_test_se se-multitable-silo-contention-ronly-sensivity.log 1 100
#do_test_se se-multitable-silo-contention-wonly-sensivity.log 1 0
do_test_zipf se-multitable-silo-contention-wonly-sensivity.log 1 0

#nowait rwlock
make clean
make "DFLAGS=-DSHARED_EVERYTHING -DENABLE_NOWAIT_CC -DSE_LATCH -DRW_LOCK -DCUSTOM_RW_LOCK $DBGFLAG"
#do_test_se se-multitable-nowait-rwlock-contention-ronly-sensivity.log 1 100
#do_test_se se-multitable-nowait-rwlock-contention-wonly-sensivity.log 1 0
do_test_zipf se-multitable-nowait-rwlock-contention-wonly-sensivity.log 1 0

#dl-detect default
make clean
make "DFLAGS=-DSHARED_EVERYTHING -DSE_LATCH -DPTHREAD_SPINLOCK -DENABLE_DL_DETECT_CC -DENABLE_CYCLE_DETECTION $DBGFLAG"
#do_test_se se-multitable-dl-detect-contention-ronly-sensivity.log 1 100
#do_test_se se-multitable-dl-detect-contention-wonly-sensivity.log 1 0
do_test_zipf se-multitable-default-dl-detect-contention-wonly-sensivity.log 1 0

#dl-detect dreadlock
#make clean
#make "DFLAGS=-DSHARED_EVERYTHING -DSE_LATCH -DPTHREAD_SPINLOCK -DENABLE_DL_DETECT_CC $DBGFLAG"
#do_test_zipf se-multitable-dreadlock-dl-detect-contention-wonly-sensivity.log 1 0

#dl-detect optimized svdreadlock
#make clean
#make "DFLAGS=-DSHARED_EVERYTHING -DSE_LATCH -DPTHREAD_SPINLOCK -DENABLE_SVDREADLOCK_CC $DBGFLAG"
#do_test_zipf se-multitable-rdoptimized-dl-detect-contention-wonly-sensivity.log 1 0

#dl-detect mv dldetect
#make clean
#make "DFLAGS=-DSHARED_EVERYTHING -DSE_LATCH -DPTHREAD_SPINLOCK -DENABLE_MVDREADLOCK_CC $DBGFLAG"
#do_test_zipf se-multitable-mvdl-detect-contention-wonly-sensivity.log 1 0

#make clean
make "DFLAGS=-DSHARED_EVERYTHING -DSE_LATCH -DPTHREAD_SPINLOCK -DENABLE_MV2PL $DBGFLAG"
#do_test_se se-multitable-mv2pl-contention-ronly-sensivity.log 1 100
#do_test_se se-multitable-mv2pl-contention-wonly-sensivity.log 1 0
do_test_zipf se-multitable-mv2pl-contention-wonly-sensivity.log 1 0



###############################################################################
#                               sn                                           #
###############################################################################
#sn
make clean
make "DFLAGS=-DSHARED_NOTHING -DPTHREAD_SPINLOCK $DBGFLAG"
#do_test_sn sn-multitable-contention-ronly-sensivity.log 1 100
do_test_zipf sn-multitable-zipf-contention-wonly-sensivity.log 1 0

###############################################################################
#                               Messaging test                                #
###############################################################################
#nowait msg
make clean
make "DFLAGS=-DENABLE_NOWAIT_CC $DBGFLAG"
#do_test_sn tr-nowait-b1-multitable-contention-ronly-sensivity.log 1 100
#do_test_sn tr-nowait-b4-multitable-contention-ronly-sensivity.log 4 100
do_test_zipf tr-nowait-b1-multitable-zipf-contention-wonly-sensivity.log 1 0
#do_test_zipf tr-nowait-b4-multitable-zipf-contention-wonly-sensivity.log 4 0

#silo msg
make clean
make "DFLAGS=-DENABLE_SILO_CC $DBGFLAG"
#do_test_sn tr-silo-b1-multitable-contention-ronly-sensivity.log 1 100
#do_test_sn tr-silo-b4-multitable-contention-ronly-sensivity.log 4 100
do_test_zipf tr-silo-b1-multitable-zipf-contention-wonly-sensivity.log 1 0
#do_test_zipf tr-silo-b4-multitable-zipf-contention-wonly-sensivity.log 4 0

#dl-detect msg
make clean
make "DFLAGS=-DENABLE_DL_DETECT_CC $DBGFLAG"
#do_test_dl_detect tr-dl-detect-b1-multitable-contention-ronly-sensivity.log 1 100
#do_test_dl_detect tr-dl-detect-b4-multitable-contention-ronly-sensivity.log 4 100
do_test_zipf tr-dl-detect-b1-multitable-zipf-contention-wonly-sensivity.log 1 0
#do_test_zipf tr-dl-detect-b4-multitable-zipf-contention-wonly-sensivity.log 4 0

#mv2pl msg
make clean
make "DFLAGS=-DENABLE_MV2PL $DBGFLAG"
do_test_zipf tr-mv2pl-b1-multitable-zipf-contention-wonly-sensivity.log 1 0
#do_test_zipf tr-mv2pl-b4-multitable-zipf-contention-wonly-sensivity.log 4 0


###############################################################################
#                               fiber test                                    #
###############################################################################

#dletect fiber
make clean
make "DFLAGS=-DSHARED_EVERYTHING -DSE_LATCH -DPTHREAD_SPINLOCK -DENABLE_DL_DETECT_CC -DMIGRATION -DNOLATCH $DBGFLAG"
#do_test_sn fiber-multitable-dldetect-contention-b1-ronly-sensivity.log 1 100
#do_test_sn fiber-multitable-dldetect-contention-b4-ronly-sensivity.log 4 100
#do_test_sn fiber-multitable-dldetect-contention-wonly-sensivity.log 1 0
#do_test_zipf fiber-multitable-dldetect-contention-b1-ronly-sensivity.log 1 100
#do_test_zipf fiber-multitable-dldetect-contention-b4-ronly-sensivity.log 4 100
do_test_zipf fiber-b1-multitable-dldetect-contention-wonly-sensivity.log 1 0
#do_test_zipf fiber-b4-multitable-dldetect-contention-wonly-sensivity.log 4 0

#nowait fiber
make clean
make "DFLAGS=-DSHARED_EVERYTHING -DSE_LATCH -DPTHREAD_SPINLOCK -DENABLE_NOWAIT_CC -DMIGRATION -DNOLATCH $DBGFLAG"
#do_test_sn fiber-multitable-nowait-default-contention-b1-ronly-sensivity.log 1 100
#do_test_sn fiber-multitable-nowait-default-contention-b4-ronly-sensivity.log 4 100
#do_test_sn fiber-multitable-nowait-default-contention-wonly-sensivity.log 1 0
#do_test_zipf fiber-multitable-nowait-default-contention-b1-ronly-sensivity.log 1 100
#do_test_zipf fiber-multitable-nowait-default-contention-b4-ronly-sensivity.log 4 100
do_test_zipf fiber-b1-multitable-nowait-default-contention-wonly-sensivity.log 1 0
#do_test_zipf fiber-b4-multitable-nowait-default-contention-wonly-sensivity.log 4 0


