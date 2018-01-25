make clean
make "DFLAGS=-DSHARED_EVERYTHING -DENABLE_NOWAIT_CC -DSE_LATCH -DSE_INDEX_LATCH -DPTHREAD_SPINLOCK "
./trireme -s 4 -b 2 > n4.txt
./trireme -s 8 -b 2 > n8.txt
./trireme -s 12 -b 2 > n12.txt
./trireme -s 16 -b 2 > n16.txt
./trireme -s 20 -b 2 > n20.txt
./trireme -s 24 -b 2 > n24.txt
make clean
make "DFLAGS=-DSHARED_EVERYTHING -DENABLE_DL_DETECT_CC -DSE_LATCH -DSE_INDEX_LATCH -DPTHREAD_SPINLOCK "
./trireme -s 4 -b 2 > d4.txt
./trireme -s 8 -b 2 > d8.txt
./trireme -s 12 -b 2 > d12.txt
./trireme -s 16 -b 2 > d16.txt
./trireme -s 20 -b 2 > d20.txt
./trireme -s 24 -b 2 > d24.txt

