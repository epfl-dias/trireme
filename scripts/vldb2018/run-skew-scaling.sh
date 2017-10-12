#!/bin/bash
do_test_se ()
{
    outfile=$1
    batchsize=$2
    ronly=$3

    for i in {1,2,4,8,18,36,54,72}; do
        if [ $ronly -eq 100 ]; then
            write=0
        else
            write=$i
        fi

        for j in {1,2,3,4,5}; do
            echo ./ycsb -s $i -d 0 -b $batchsize -o 10 -t 100000000 -w $write -a 100 -h 16 -p 1
            echo ./ycsb -s $i -d 0 -b $batchsize -o 10 -t 100000000 -w $write -a 100 -h 16 -p 1 >> $outfile
            ./ycsb -s $i -d 0 -b $batchsize -o 10 -t 100000000 -w $write -a 100 -h 16 -p 1 >> $outfile
        done
    done
}

do_test_sn ()
{
    outfile=$1
    batchsize=$2
    ronly=$3

    for j in {1,2,3,4,5}; do
        if [ $ronly -eq 100 ]; then
            write=0
        else
            write=1
        fi

        echo ./ycsb -s 1 -d 0 -b $batchsize -o 10 -t 100000000 -w $write -a 100 -h 16 -p 1
        echo ./ycsb -s 1 -d 0 -b $batchsize -o 10 -t 100000000 -w $write -a 100 -h 16 -p 1 >> $outfile
        ./ycsb -s 1 -d 0 -b $batchsize -o 10 -t 100000000 -w $write -a 100 -h 16 -p 1 >> $outfile
    done

    for j in {1,2,3,4,5}; do
        if [ $ronly -eq 100 ]; then
            write=0
        else
            write=2
        fi

        echo ./ycsb -s 2 -d 0 -b $batchsize -o 10 -t 100000000 -w $write -a 100 -h 8 -p 2
        echo ./ycsb -s 2 -d 0 -b $batchsize -o 10 -t 100000000 -w $write -a 100 -h 8 -p 2 >> $outfile
        ./ycsb -s 2 -d 0 -b $batchsize -o 10 -t 100000000 -w $write -a 100 -h 8 -p 2 >> $outfile
    done

    for j in {1,2,3,4,5}; do
        if [ $ronly -eq 100 ]; then
            write=0
        else
            write=4
        fi

        echo ./ycsb -s 4 -d 0 -b $batchsize -o 10 -t 100000000 -w $write -a 100 -h 4 -p 4
        echo ./ycsb -s 4 -d 0 -b $batchsize -o 10 -t 100000000 -w $write -a 100 -h 4 -p 4 >> $outfile
        ./ycsb -s 4 -d 0 -b $batchsize -o 10 -t 100000000 -w $write -a 100 -h 4 -p 4 >> $outfile
    done

    for j in {1,2,3,4,5}; do
        if [ $ronly -eq 100 ]; then
            write=0
        else
            write=8
        fi

        echo ./ycsb -s 8 -d 0 -b $batchsize -o 10 -t 100000000 -w $write -a 100 -h 2 -p 8
        echo ./ycsb -s 8 -d 0 -b $batchsize -o 10 -t 100000000 -w $write -a 100 -h 2 -p 8 >> $outfile
        ./ycsb -s 8 -d 0 -b $batchsize -o 10 -t 100000000 -w $write -a 100 -h 2 -p 8 >> $outfile
    done

    for i in {18,36,54,72}; do
        if [ $ronly -eq 100 ]; then
            write=0
        else
            write=$i
        fi

        for j in {1,2,3,4,5}; do
            echo ./ycsb -s $i -d 0 -b $batchsize -o 10 -t 100000000 -w $write -a 100 -h 1 -p 16
            echo ./ycsb -s $i -d 0 -b $batchsize -o 10 -t 100000000 -w $write -a 100 -h 1 -p 16 >> $outfile
            ./ycsb -s $i -d 0 -b $batchsize -o 10 -t 100000000 -w $write -a 100 -h 1 -p 16 >> $outfile
        done
    done
}

do_se_tests() {
    ###############################################################################
    #                               latching test                                 #
    ###############################################################################
    #silo spinlock
    #make clean
    #make "DFLAGS=-DSHARED_EVERYTHING -DSE_LATCH -DPTHREAD_SPINLOCK -DENABLE_SILO_CC "
    #do_test_se se-multitable-silo-ronly-skew-scaling.log 1 100
    #do_test_se se-multitable-silo-wonly-conflict-scaling.log 1 0

    #nowait rwlock
    #make clean
    #make "DFLAGS=-DSHARED_EVERYTHING -DENABLE_NOWAIT_CC -DSE_LATCH -DRW_LOCK -DCUSTOM_RW_LOCK "
    #do_test_se se-multitable-nowait-rwlock-ronly-skew-scaling.log 1 100
    #do_test_se se-multitable-nowait-rwlock-wonly-conflict-scaling.log 1 0

    #dl-detect default
    #make clean
    #make "DFLAGS=-DSHARED_EVERYTHING -DSE_LATCH -DPTHREAD_SPINLOCK -DENABLE_DL_DETECT_CC -DENABLE_CYCLE_DETECTION "
    #do_test_se se-multitable-default-dl-detect-ronly-skew-scaling.log 1 100
    #do_test_se se-multitable-default-dl-detect-wonly-conflict-scaling.log 1 0

    #dl-detect dreadlock
    #make clean
    #make "DFLAGS=-DSHARED_EVERYTHING -DSE_LATCH -DPTHREAD_SPINLOCK -DENABLE_DL_DETECT_CC "
    #do_test_se se-multitable-dreadlock-dl-detect-ronly-skew-scaling.log 1 100
    #do_test_se se-multitable-dreadlock-dl-detect-wonly-conflict-scaling.log 1 0

    #dl-detect optimized svdreadlock
    #make clean
    #make "DFLAGS=-DSHARED_EVERYTHING -DSE_LATCH -DPTHREAD_SPINLOCK -DENABLE_SVDREADLOCK_CC "
    #do_test_se se-multitable-rdoptimized-dl-detect-ronly-skew-scaling.log 1 100
    #do_test_se se-multitable-rdoptimized-dl-detect-wonly-conflict-scaling.log 1 0

    #dl-detect optimized mvdreadlock
    #make clean
    #make "DFLAGS=-DSHARED_EVERYTHING -DSE_LATCH -DPTHREAD_SPINLOCK -DENABLE_MVDREADLOCK_CC "
    #do_test_se se-multitable-mvdl-detect-ronly-skew-scaling.log 1 100
    #do_test_se se-multitable-mvdl-detect-wonly-conflict-scaling.log 1 0

    #mv2pl nowait
    #make clean
    #make "DFLAGS=-DSHARED_EVERYTHING -DSE_LATCH -DPTHREAD_SPINLOCK -DENABLE_MV2PL "
    #do_test_se se-multitable-mv2pl-ronly-skew-scaling.log 1 100
    #do_test_se se-multitable-mv2pl-wonly-conflict-scaling.log 1 0

    #mv2pl nowait_drwlock
    make clean
    make "DFLAGS=-DSHARED_EVERYTHING -DSE_LATCH -DPTHREAD_SPINLOCK -DENABLE_MV2PL_DRWLOCK "
    do_test_se se-multitable-mv2pl-drwlock-ronly-skew-scaling.log 1 100
    do_test_se se-multitable-mv2pl-drwlock-wonly-conflict-scaling.log 1 0
}

do_sn_tests() {
    ###############################################################################
    #                               sn                                           #
    ###############################################################################
    #sn
    make clean
    make "DFLAGS=-DSHARED_NOTHING -DPTHREAD_SPINLOCK "
    do_test_sn sn-multitable-ronly-skew-scaling.log 1 100
    do_test_sn sn-multitable-wonly-conflict-scaling.log 1 0
}

do_msg_tests() {
    #
    ###############################################################################
    #                               Messaging test                                #
    ###############################################################################
    #nowait msg
    #make clean
    #make "DFLAGS=-DENABLE_NOWAIT_CC "
    #do_test_sn tr-nowait-b1-multitable-ronly-skew-scaling.log 1 100
    #do_test_sn tr-nowait-b4-multitable-ronly-skew-scaling.log 4 100
    #do_test_sn tr-nowait-b1-multitable-wonly-conflict-scaling.log 1 0

    #dl-detect msg
    #make clean
    #make "DFLAGS=-DENABLE_DL_DETECT_CC "
    #do_test_sn tr-dl-detect-b1-multitable-ronly-skew-scaling.log 1 100
    #do_test_sn tr-dl-detect-b4-multitable-ronly-skew-scaling.log 4 100
    #do_test_sn tr-dl-detect-b1-multitable-wonly-conflict-scaling.log 1 0

    #silo msg
    make clean
    make "DFLAGS=-DENABLE_SILO_CC "
    #do_test_sn tr-silo-b1-multitable-ronly-skew-scaling.log 1 100
    #do_test_sn tr-silo-b4-multitable-ronly-skew-scaling.log 4 100
    do_test_sn tr-silo-b1-multitable-wonly-conflict-scaling.log 1 0

    #mv2pl msg
    #make clean
    #make "DFLAGS=-DENABLE_MV2PL "
    #do_test_sn tr-silo-b1-multitable-ronly-skew-scaling.log 1 100
    #do_test_sn tr-mv2pl-b4-multitable-ronly-skew-scaling.log 4 100
    #do_test_sn tr-mv2pl-b1-multitable-wonly-conflict-scaling.log 1 0
}

do_fiber_tests() {
    ###############################################################################
    #                               fiber test                                    #
    ###############################################################################

    #dletect fiber
    make clean
    make "DFLAGS=-DSHARED_EVERYTHING -DSE_LATCH -DPTHREAD_SPINLOCK -DENABLE_DL_DETECT_CC -DENABLE_CYCLE_DETECTION -DMIGRATION -DNOLATCH "
    #do_test_sn fiber-multitable-b1-dldetect-ronly-skew-scaling.log 1 100
    do_test_sn fiber-multitable-b4-dldetect-ronly-skew-scaling.log 4 100
    do_test_sn fiber-multitable-b1-dldetect-wonly-conflict-scaling.log 1 0

    #nowait fiber
    make clean
    make "DFLAGS=-DSHARED_EVERYTHING -DSE_LATCH -DPTHREAD_SPINLOCK -DENABLE_NOWAIT_CC -DMIGRATION -DNOLATCH "
    #do_test_sn fiber-multitable-b1-nowait-default-ronly-skew-scaling.log 1 100
    do_test_sn fiber-multitable-b4-nowait-default-ronly-skew-scaling.log 4 100
    do_test_sn fiber-multitable-nowait-default-wonly-conflict-scaling.log 1 0
}

cd ../..

#DBGFLAG="-DYCSB_BENCHMARK"
#DBGFLAG="-DGATHER_STATS"

do_se_tests
#do_sn_tests
#do_msg_tests
#do_fiber_tests

###############################################################################
#                         key ordering tests                                  #
###############################################################################

#silo spinlock
#make clean
#make "DFLAGS=-DSHARED_EVERYTHING -DSE_LATCH -DPTHREAD_SPINLOCK -DENABLE_SILO_CC -DENABLE_KEY_SORTING $DBGFLAG"
#do_test_zipfian se-multitable-silo-wonly-keysort-conflict-scaling.log 1 0

#nowait rwlock
#make clean
#make "DFLAGS=-DSHARED_EVERYTHING -DENABLE_NOWAIT_CC -DSE_LATCH -DRW_LOCK -DCUSTOM_RW_LOCK -DENABLE_KEY_SORTING $DBGFLAG"
#do_test_zipfian se-multitable-nowait-rwlock-wonly-keysort-conflict-scaling.log 1 0

#dl-detect default
#make clean
#make "DFLAGS=-DSHARED_EVERYTHING -DSE_LATCH -DPTHREAD_SPINLOCK -DENABLE_DL_DETECT_CC -DENABLE_CYCLE_DETECTION -DENABLE_KEY_SORTING $DBGFLAG"
#do_test_zipfian se-multitable-zipfian-default-dl-detect-keysort-conflict-scaling.log 1 0
#do_test_se se-multitable-default-dl-detect-keysort-conflict-scaling.log 1 0

#dl-detect dreadlock
#make clean
#make "DFLAGS=-DSHARED_EVERYTHING -DSE_LATCH -DPTHREAD_SPINLOCK -DENABLE_DL_DETECT_CC -DENABLE_KEY_SORTING $DBGFLAG"
#do_test_zipfian se-multitable-zipfian-dreadlock-dl-detect-keysort-conflict-scaling.log 1 0
#do_test_se se-multitable-dreadlock-dl-detect-keysort-conflict-scaling.log 1 0

#dl-detect optimized svdreadlock
#make clean
#make "DFLAGS=-DSHARED_EVERYTHING -DSE_LATCH -DPTHREAD_SPINLOCK -DENABLE_SVDREADLOCK_CC -DENABLE_KEY_SORTING $DBGFLAG"
#do_test_zipfian se-multitable-zipfian-rdoptimized-dl-detect-keysort-conflict-scaling.log 1 0
#do_test_se se-multitable-rdoptimized-dl-detect-keysort-conflict-scaling.log 1 0

#dl-detect optimized mvdreadlock
#make clean
#make "DFLAGS=-DSHARED_EVERYTHING -DSE_LATCH -DPTHREAD_SPINLOCK -DENABLE_MVDREADLOCK_CC -DENABLE_KEY_SORTING $DBGFLAG"
#do_test_zipfian se-multitable-zipfian-mvdl-detect-keysort-conflict-scaling.log 1 0
#do_test_se se-multitable-mvdl-detect-keysort-conflict-scaling.log 1 0

#sn
#make clean
#make "DFLAGS=-DSHARED_NOTHING -DPTHREAD_SPINLOCK -DENABLE_KEY_SORTING $DBGFLAG"
#do_test_zipfian sn-multitable-wonly-keysort-conflict-scaling.log 1 0

#nowait msg
#make clean
#make "DFLAGS=-DENABLE_NOWAIT_CC -DENABLE_KEY_SORTING $DBGFLAG"
#do_test_zipfian tr-nowait-b1-multitable-wonly-keysort-conflict-scaling.log 1 0

#dl-detect msg
#make clean
#make "DFLAGS=-DENABLE_DL_DETECT_CC -DENABLE_KEY_SORTING $DBGFLAG"
#do_test_zipfian tr-dl-detect-b1-multitable-wonly-keysort-conflict-scaling.log 1 0
#do_test_sn tr-dl-detect-b1-multitable-wonly-keysort-conflict-scaling.log 1 0
#do_test_sn tr-dl-detect-b4-multitable-wonly-keysort-conflict-scaling.log 4 0

#dletect fiber
#make clean
#make "DFLAGS=-DSHARED_EVERYTHING -DSE_LATCH -DPTHREAD_SPINLOCK -DENABLE_DL_DETECT_CC -DMIGRATION -DNOLATCH -DENABLE_KEY_SORTING $DBGFLAG"
#do_test_zipfian fiber-multitable-dldetect-wonly-keysort-conflict-scaling.log 1 0

