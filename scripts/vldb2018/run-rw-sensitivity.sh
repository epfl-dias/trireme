#!/bin/bash
do_test_micro () {
  outfile=$1

  for i in {0,1,18,36,54,72}; do
      for j in {1,2,3}; do
          echo ./ycsb -s 72 -d 0 -b 1 -o 16 -t 100000000 -w $i -h 16 -a 100 -p 1
          echo ./ycsb -s 72 -d 0 -b 1 -o 16 -t 100000000 -w $i -h 16 -a 100 -p 1 >> $outfile
          ./ycsb -s 72 -d 0 -b 1 -o 16 -t 100000000 -w $i -h 16 -a 100 -p 1 >> $outfile
      done
  done
}

do_test_zipf_ycsb () {
  outfile=$1
  b=$2

  for i in {0,1,18,36,54,72}; do
      for j in {1,2,3,4,5}; do
          echo ./ycsb -s 72 -d 0 -b $b -o 16 -t 20000000 -w $i
          echo ./ycsb -s 72 -d 0 -b $b -o 16 -t 20000000 -w $i >> $outfile
          ./ycsb -s 72 -d 0 -b $b -o 16 -t 20000000 -w $i >> $outfile
      done
  done
}

cd ../..

DBGFLAGS="-DYCSB_BENCHMARK"

###############################################################################
#                               latching test                                 #
###############################################################################
#silo spinlock
#make clean
#make "DFLAGS=-DSHARED_EVERYTHING -DSE_LATCH -DPTHREAD_SPINLOCK -DENABLE_SILO_CC $DBGFLAGS"
#do_test_zipf_ycsb se-silo-rw-sensitivity.log 1

#nowait rwlock
#make clean
#make "DFLAGS=-DSHARED_EVERYTHING -DENABLE_NOWAIT_CC -DSE_LATCH -DRW_LOCK -DCUSTOM_RW_LOCK $DBGFLAGS"
#do_test_zipf_ycsb se-nowait-rwlock-rw-sensitivity.log 1

#dl-detect default
#make clean
#make "DFLAGS=-DSHARED_EVERYTHING -DSE_LATCH -DPTHREAD_SPINLOCK -DENABLE_DL_DETECT_CC -DENABLE_CYCLE_DETECTION $DBGFLAGS"
#do_test_zipf_ycsb se-default-dldetect-rw-sensitivity.log 1

#dl-detect dreadlock
#make clean
#make "DFLAGS=-DSHARED_EVERYTHING -DSE_LATCH -DPTHREAD_SPINLOCK -DENABLE_DL_DETECT_CC $DBGFLAGS"
#do_test_zipf_ycsb se-dreadlock-dldetect-rw-sensitivity.log 1

#dl-detect optimized svdreadlock
#make clean
#make "DFLAGS=-DSHARED_EVERYTHING -DSE_LATCH -DPTHREAD_SPINLOCK -DENABLE_SVDREADLOCK_CC $DBGFLAGS"
#do_test_zipf_ycsb se-rdoptimized-dldetect-rw-sensitivity.log 1

#dl-detect optimized mvdreadlock
#make clean
#make "DFLAGS=-DSHARED_EVERYTHING -DSE_LATCH -DPTHREAD_SPINLOCK -DENABLE_MVDREADLOCK_CC $DBGFLAGS"
#do_test_zipf_ycsb se-mvdldetect-rw-sensitivity.log 1

#2v2pl test
#make clean
#make "DFLAGS=-DSHARED_EVERYTHING -DSE_LATCH -DPTHREAD_SPINLOCK -DENABLE_MV2PL $DBGFLAGS"
#do_test_zipf_ycsb se-mv2pl-rw-sensitivity.log 1

#2v2pl_drwlock test
make clean
make "DFLAGS=-DSHARED_EVERYTHING -DSE_LATCH -DPTHREAD_SPINLOCK -DENABLE_MV2PL_DRWLOCK $DBGFLAGS"
do_test_zipf_ycsb se-mv2pl-drwlock-rw-sensitivity.log 1

###############################################################################
#                               sn                                           #
###############################################################################
#sn
#make clean
#make "DFLAGS=-DSHARED_NOTHING -DPTHREAD_SPINLOCK $DBGFLAGS"
#do_test_zipf_ycsb sn-zipf-rw-sensivity.log 1

###############################################################################
#                               Messaging test                                #
###############################################################################
#nowait msg
#make clean
#make "DFLAGS=-DENABLE_NOWAIT_CC $DBGFLAGS"
#do_test_zipf_ycsb tr-nowait-b1-multitable-zipf-rw-sensivity.log 1
#do_test_zipf_ycsb tr-nowait-b4-multitable-zipf-rw-sensivity.log 4

#silo msg
#make clean
#make "DFLAGS=-DENABLE_SILO_CC $DBGFLAGS"
#do_test_zipf_ycsb tr-silo-b1-multitable-zipf-rw-sensivity.log 1
#do_test_zipf_ycsb tr-silo-b4-multitable-zipf-rw-sensivity.log 4

#dl-detect msg
#make clean
#make "DFLAGS=-DENABLE_DL_DETECT_CC $DBGFLAGS"
#do_test_zipf_ycsb tr-dl-detect-b1-multitable-zipf-rw-sensivity.log 1
#do_test_zipf_ycsb tr-dl-detect-b4-multitable-zipf-rw-sensivity.log 4

#mv2pl msg
#make clean
#make "DFLAGS=-DENABLE_MV2PL $DBGFLAGS"
#do_test_zipf_ycsb tr-mv2pl-b1-multitable-zipf-rw-sensivity.log 1
#do_test_zipf_ycsb tr-mv2pl-b4-multitable-zipf-rw-sensivity.log 4
