make clean
make "DFLAGS=-DENABLE_WAIT_DIE_CC "

for i in {64,32,16,8,4,2,1}; do
#  for j in {1,2,3}; do
    echo ./ycsb -s $i -d 100 -b 1 -i 1000000 -o 10 -t `expr 64 \* 1000000` -w 100 >> tr.tpcc.b1.neworder.all-local.log
    ./ycsb -s $i -d 100 -b 1 -i 1000000 -o 10 -t `expr 64 \* 1000000` -w 100 >> tr.tpcc.b1.neworder.all-local.log
    #echo ./ycsb -s $i -d 0 -b 4 -i 1000000 -o 10 -t `expr 64 \* 1000000` -w 100 >> tr.tpcc.b4.neworder-with-batching.log
    #./ycsb -s $i -d 0 -b 4 -i 1000000 -o 10 -t `expr 64 \* 1000000` -w 100 >> tr.tpcc.b4.neworder-with-batching.log
 # done
#done

make clean
make "DFLAGS=-DSHARED_NOTHING -DPTHREAD_SPINLOCK"

    echo ./ycsb -s $i -d 100 -b 1 -i 1000000 -o 10 -t `expr 64 \* 1000000` -w 100 >> sn.tpcc.b1.neworder.all-local.log
    ./ycsb -s $i -d 100 -b 1 -i 1000000 -o 10 -t `expr 64 \* 1000000` -w 100 >> sn.tpcc.b1.neworder.all-local.log

#for i in {1,2,4,8,16,32,64}; do
 # for j in {1,2,3}; do
#    echo ./ycsb -s $i -d 0 -b 1 -i 1000000 -o 10 -t `expr 64 \* 1000000` -w 100 >> sn.tpcc.neworder.log
#    ./ycsb -s $i -d 0 -b 1 -i 1000000 -o 10 -t `expr 64 \* 1000000` -w 100 >> sn.tpcc.neworder.log
 # done
#done


make clean
make "DFLAGS=-DSHARED_EVERYTHING -DSE_LATCH -DPTHREAD_SPINLOCK -DENABLE_WAIT_DIE_CC "

    echo ./ycsb -s $i -d 100 -b 1 -i 1000000 -o 10 -t `expr 64 \* 1000000` -w 100 >> se.tpcc.b1.neworder.all-local.log
    ./ycsb -s $i -d 100 -b 1 -i 1000000 -o 10 -t `expr 64 \* 1000000` -w 100 >> se.tpcc.b1.neworder.all-local.log
#for i in {1,2,4,8,16,32,64}; do
 # for j in {1,2,3}; do
#    echo ./ycsb -s $i -d 0 -b 1 -i 1000000 -o 10 -t `expr 64 \* 1000000` -w 100 >> se.tpcc.neworder.log
#    ./ycsb -s $i -d 0 -b 1 -i 1000000 -o 10 -t `expr 64 \* 1000000` -w 100 >> se.tpcc.neworder.log
 # done
#done

done
