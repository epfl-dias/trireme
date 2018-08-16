do_test () {
  outfile=$1
  batchsize=$2

  for r in {1,2,3,4,5,6,7,8,9,10}; do
    for j in {1,2,3}; do
      echo ./ycsb -s 64 -d 80 -b $batchsize -i 10000000 -o 10 -t 64000000 -w 100 -r $r >> $outfile
      ./ycsb -s 64 -d 80 -b $batchsize -i 10000000 -o 10 -t 64000000 -w 100 -r $r >> $outfile
    done
  done
}

make clean
make "DFLAGS=-DENABLE_WAIT_DIE_CC "
#do_test tr-b1-dist-op-sensitivity-no-batching.log 1
do_test tr-d80-b4-dist-op-sensitivity-no-batching.log 4

make clean
make "DFLAGS=-DENABLE_WAIT_DIE_CC -DENABLE_OP_BATCHING"
#do_test tr-b1-dist-op-sensitivity-with-batching.log 1
do_test tr-d80-b4-dist-op-sensitivity-with-batching.log 4

make clean
make "DFLAGS=-DSHARED_NOTHING -DPTHREAD_SPINLOCK"
do_test sn-d80-dist-op-sensitivity.log 1

make clean
make "DFLAGS=-DSHARED_EVERYTHING -DSE_LATCH -DPTHREAD_SPINLOCK -DENABLE_WAIT_DIE_CC "
do_test se-d80-dist-op-sensitivity.log 1
