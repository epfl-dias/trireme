#se_no_wait
#make clean
#make "DFLAGS=-DSHARED_EVERYTHING -DENABLE_NOWAIT_CC -DSE_LATCH -DSE_INDEX_LATCH -DPTHREAD_SPINLOCK "
#./trireme -s 1 -b 2 >&1 2>SE_NW_1
#./trireme -s 4 -b 2 >&1 2>SE_NW_4
#./trireme -s 8 -b 2 >&1 2>SE_NW_8
#./trireme -s 12 -b 2 >&1 2>SE_NW_12
#./trireme -s 16 -b 2 >&1 2>SE_NW_16
#./trireme -s 20 -b 2 >&1 2>SE_NW_20
#./trireme -s 24 -b 2 >&1 2>SE_NW_24
#se_dl_detect
#make clean
#make "DFLAGS=-DSHARED_EVERYTHING -DENABLE_DL_DETECT_CC -DSE_LATCH -DSE_INDEX_LATCH -DPTHREAD_SPINLOCK "
#./trireme -s 1 -b 2 >&1 2>SE_DL_1
#./trireme -s 4 -b 2 >&1 2>SE_DL_4
#./trireme -s 8 -b 2 >&1 2>SE_DL_8
#./trireme -s 12 -b 2 >&1 2>SE_DL_12
#./trireme -s 16 -b 2 >&1 2>SE_DL_16
#./trireme -s 20 -b 2 >&1 2>SE_DL_20
#./trireme -s 24 -b 2 >&1 2>SE_DL_24
#sn
#make clean
#make "DFLAGS=-DSHARED_NOTHING -DPTHREAD_SPINLOCK -DSE_INDEX_LATCH -DPTHREAD_SPINLOCK "
#./trireme -s 1 -b 2 >&1 2>SN_1
#./trireme -s 4 -b 2 >&1 2>SN_4
#./trireme -s 8 -b 2 >&1 2>SN_8 
#./trireme -s 12 -b 2 >&1 2>SN_12
#./trireme -s 16 -b 2 >&1 2>SN_16
#./trireme -s 20 -b 2 >&1 2>SN_20
#./trireme -s 24 -b 2 >&1 2>SN_24
#delegation_no_wait
make clean
#make "DFLAGS=-DENABLE_NOWAIT_CC "
make "DFLAGS=-DENABLE_NOWAIT_CC -DSE_INDEX_LATCH -DPTHREAD_SPINLOCK"
#./trireme -s 1 -b 2 >&1 2>DEL_NW_1
./trireme -s 4 -b 2 >&1 2>DEL_NW_4
./trireme -s 8 -b 2 >&1 2>DEL_NW_8
./trireme -s 12 -b 2 >&1 2>DEL_NW_12
./trireme -s 16 -b 2 >&1 2>DEL_NW_16
./trireme -s 20 -b 2 >&1 2>DEL_NW_20
./trireme -s 24 -b 2 >&1 2>DEL_NW_24
#delegation_dl_detect
make clean
#make "DFLAGS=-DENABLE_DL_DETECT_CC"
make "DFLAGS=-DENABLE_DL_DETECT_CC -DSE_INDEX_LATCH -DPTHREAD_SPINLOCK"
#./trireme -s 1 -b 2 >&1 2>DEL_DL_1
./trireme -s 4 -b 2 >&1 2>DEL_DL_4
./trireme -s 8 -b 2 >&1 2>DEL_DL_8
./trireme -s 12 -b 2 >&1 2>DEL_DL_12
./trireme -s 16 -b 2 >&1 2>DEL_DL_16
./trireme -s 20 -b 2 >&1 2>DEL_DL_20
./trireme -s 24 -b 2 >&1 2>DEL_DL_24