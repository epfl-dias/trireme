do_test () {
  outfile=$1
  batchsize=$2

  for i in {0,10,20,30,40,50,60,70,80,90,100}; do
    for j in {1,2,3}; do
      echo ./ycsb -s 16 -d `expr 100 - $i` -b $batchsize -i 10000000 -o 10 -t 16000000 -w 100 >> $outfile
      ./ycsb -s 16 -d `expr 100 - $i` -b $batchsize -i 10000000 -o 10 -t 16000000 -w 100 >> $outfile
    done
  done
}

make clean
make "DFLAGS=-DENABLE_WAIT_DIE_CC "
do_test tr-dist-tran-sensitivity-b1-10inc-16cores.log 1
do_test tr-dist-tran-sensitivity-b4-10inc-16cores.log 4

make clean
make "DFLAGS=-DSHARED_NOTHING -DPTHREAD_SPINLOCK"
do_test sn-dist-tran-sensitivity-10inc-16cores.log 1

make clean
make "DFLAGS=-DSHARED_EVERYTHING -DSE_LATCH -DPTHREAD_SPINLOCK -DENABLE_WAIT_DIE_CC "
do_test se-dist-tran-sensitivity-10inc-16cores.log 1
