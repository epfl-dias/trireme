do_test_se () {
  outfile=$1
  batchsize=$2
  write=$3

  for k in {1,2,3}; do
      echo ./ycsb -s 36 -d 0 -b $batchsize -i 100000000 -o 10 -t 72 -w $write
      echo ./ycsb -s 36 -d 0 -b $batchsize -i 100000000 -o 10 -t 72 -w $write >> $outfile
      ./ycsb -s 36 -d 0 -b $batchsize -i 100000000 -o 10 -t 72 -w $write >> $outfile
  done
}

do_test_sn () {
  outfile=$1
  batchsize=$2
  write=$3

  for k in {1,2,3}; do
      echo ./ycsb -s 36 -d 0 -b $batchsize -i 100000000 -o 10 -r 10 -t 72 -w $write
      echo ./ycsb -s 36 -d 0 -b $batchsize -i 100000000 -o 10 -r 10 -t 72 -w $write >> $outfile
      ./ycsb -s 36 -d 0 -b $batchsize -i 100000000 -o 10 -r 10 -t 72 -w $write >> $outfile
  done
}

cd ../..

#nolatch
make clean
make "DFLAGS=-DSHARED_EVERYTHING"
do_test_se se-nowait-nolatch-contention-socket-sensitivity-twosocket.log 1 100
do_test_se se-nowait-nolatch-conflict-socket-sensitivity-twosocket.log 1 0

make clean
make "DFLAGS=-DSHARED_EVERYTHING -DHT_ENABLED"
do_test_se se-nowait-nolatch-contention-socket-sensitivity-onesocket.log 1 100
do_test_se se-nowait-nolatch-conflict-socket-sensitivity-onesocket.log 1 0

#nowait with ownerlist
make clean
make "DFLAGS=-DSHARED_EVERYTHING -DSE_LATCH -DPTHREAD_SPINLOCK -DENABLE_NOWAIT_OWNER_CC"
do_test_se se-nowait-with-ownerlist-spinlock-contention-socket-sensitivity-twosocket.log 1 100
do_test_se se-nowait-with-ownerlist-spinlock-conflict-socket-sensitivity-twosocket.log 1 0

make clean
make "DFLAGS=-DSHARED_EVERYTHING -DSE_LATCH -DPTHREAD_SPINLOCK -DHT_ENABLED -DENABLE_NOWAIT_OWNER_CC"
do_test_se se-nowait-with-ownerlist-spinlock-contention-socket-sensitivity-onesocket.log 1 100
do_test_se se-nowait-with-ownerlist-spinlock-conflict-socket-sensitivity-onesocket.log 1 0

#waitdie
make clean
make "DFLAGS=-DSHARED_EVERYTHING -DSE_LATCH -DPTHREAD_SPINLOCK -DENABLE_WAIT_DIE_CC "
do_test_se se-waitdie-spinlock-contention-socket-sensitivity-twosocket.log 1 100
do_test_se se-waitdie-spinlock-conflict-socket-sensitivity-twosocket.log 1 0

make clean
make "DFLAGS=-DSHARED_EVERYTHING -DSE_LATCH -DPTHREAD_SPINLOCK -DENABLE_WAIT_DIE_CC -DHT_ENABLED"
do_test_se se-waitdie-spinlock-contention-socket-sensitivity-onesocket.log 1 100
do_test_se se-waitdie-spinlock-conflict-socket-sensitivity-onesocket.log 1 0

#nowait with rwlock
make clean
make "DFLAGS=-DSHARED_EVERYTHING -DSE_LATCH -DRW_LOCK"
do_test_se se-nowait-pthread-rwlock-contention-socket-sensitivity-twosocket.log 1 100
do_test_se se-nowait-pthread-rwlock-conflict-socket-sensitivity-twosocket.log 1 0

make clean
make "DFLAGS=-DSHARED_EVERYTHING -DSE_LATCH -DRW_LOCK -DHT_ENABLED"
do_test_se se-nowait-pthread-rwlock-contention-socket-sensitivity-onesocket.log 1 100
do_test_se se-nowait-pthread-rwlock-conflict-socket-sensitivity-onesocket.log 1 0

#nowait with drwlock
make clean
make "DFLAGS=-DSHARED_EVERYTHING -DSE_LATCH -DDRW_LOCK"
do_test_se se-nowait-custom-drwlock-contention-socket-sensitivity-twosocket.log 1 100
do_test_se se-nowait-custom-drwlock-conflict-socket-sensitivity-twosocket.log 1 0

make clean
make "DFLAGS=-DSHARED_EVERYTHING -DSE_LATCH -DDRW_LOCK -DHT_ENABLED"
do_test_se se-nowait-custom-drwlock-contention-socket-sensitivity-onesocket.log 1 100
do_test_se se-nowait-custom-drwlock-conflict-socket-sensitivity-onesocket.log 1 0

#trireme waitdie
make clean
make "DFLAGS=-DENABLE_WAIT_DIE_CC "
do_test_sn tr-waitdie-contention-socket-sensitivity-b1-twosocket.log 1 100
do_test_sn tr-waitdie-contention-socket-sensitivity-b8-twosocket.log 8 100
do_test_sn tr-waitdie-conflict-socket-sensitivity-b1-twosocket.log 1 0
do_test_sn tr-waitdie-conflict-socket-sensitivity-b8-twosocket.log 8 0

make clean
make "DFLAGS=-DENABLE_WAIT_DIE_CC -DHT_ENABLED"
do_test_sn tr-waitdie-contention-socket-sensitivity-b1-onesocket.log 1 100
do_test_sn tr-waitdie-contention-socket-sensitivity-b8-onesocket.log 8 100
do_test_sn tr-waitdie-conflict-socket-sensitivity-b1-onesocket.log 1 0
do_test_sn tr-waitdie-conflict-socket-sensitivity-b8-onesocket.log 8 0

#trireme nowait
make clean
make
do_test_sn tr-nowait-contention-socket-sensitivity-b1-twosocket.log 1 100
do_test_sn tr-nowait-contention-socket-sensitivity-b8-twosocket.log 8 100
do_test_sn tr-nowait-conflict-socket-sensitivity-b1-twosocket.log 1 0
do_test_sn tr-nowait-conflict-socket-sensitivity-b8-twosocket.log 8 0

make clean
make "DFLAGS=-DHT_ENABLED"
do_test_sn tr-nowait-contention-socket-sensitivity-b1-onesocket.log 1 100
do_test_sn tr-nowait-contention-socket-sensitivity-b8-onesocket.log 8 100
do_test_sn tr-nowait-conflict-socket-sensitivity-b1-onesocket.log 1 0
do_test_sn tr-nowait-conflict-socket-sensitivity-b8-onesocket.log 8 0

#shared nothing
make clean
make "DFLAGS=-DSHARED_NOTHING -DPTHREAD_SPINLOCK"
do_test_sn sn-contention-socket-sensitivity-twosocket.log 1 100
do_test_sn sn-conflict-socket-sensitivity-twosocket.log 1 0

make clean
make "DFLAGS=-DSHARED_NOTHING -DPTHREAD_SPINLOCK -DHT_ENABLED"
do_test_sn sn-contention-socket-sensitivity-onesocket.log 1 100
do_test_sn sn-conflict-socket-sensitivity-onesocket.log 1 0

cd -
