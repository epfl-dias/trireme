make clean
make "DFLAGS=-DENABLE_WAIT_DIE_CC "

for i in {1,2,4,8,16,32,64}; do
  for j in {1,2,3}; do
    echo ./ycsb -s $i -d 100 -b 1 -i 10000000 -o 10 -t `expr $i \* 1000000` -w 100 >> tr.bestcase.log
    ./ycsb -s $i -d 100 -b 1 -i 10000000 -o 10 -t `expr $i \* 1000000` -w 100 >> tr.bestcase.log
  done
done


make clean
make "DFLAGS=-DSHARED_NOTHING -DPTHREAD_SPINLOCK"

for i in {1,2,4,8,16,32,64}; do
  for j in {1,2,3}; do
    echo ./ycsb -s $i -d 100 -b 1 -i 10000000 -o 10 -t `expr $i \* 1000000` -w 100 >> sn.bestcase.log
    ./ycsb -s $i -d 100 -b 1 -i 10000000 -o 10 -t `expr $i \* 1000000` -w 100 >> sn.bestcase.log
  done
done


make clean
make "DFLAGS=-DSHARED_EVERYTHING -DSE_LATCH -DPTHREAD_SPINLOCK -DENABLE_WAIT_DIE_CC "

for i in {1,2,4,8,16,32,64}; do
  for j in {1,2,3}; do
    echo ./ycsb -s $i -d 0 -b 1 -i 10000000 -o 10 -t `expr $i \* 1000000` -w 100 >> se.bestcase.log
    ./ycsb -s $i -d 0 -b 1 -i 10000000 -o 10 -t `expr $i \* 1000000` -w 100 >> se.bestcase.log
  done
done

