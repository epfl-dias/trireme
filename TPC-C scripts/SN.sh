#se_no_wait
make clean
make "DFLAGS=-DSHARED_NOTHING -DPTHREAD_SPINLOCK -DSE_INDEX_LATCH -DPTHREAD_SPINLOCK "
./trireme -s 1 -b 2 >SN_1_1 2>ERR_SN_1_1
./trireme -s 4 -b 2 >SN_4_1 2>ERR_SN_4_1
./trireme -s 8 -b 2 >SN_8_1 2>ERR_SN_8_1
./trireme -s 12 -b 2 >SN_12_1 2>ERR_SN_12_1
./trireme -s 16 -b 2 >SN_16_1 2>ERR_SN_16_1
./trireme -s 20 -b 2 >SN_20_1 2>ERR_SN_20_1
./trireme -s 24 -b 2 >SN_24_1 2>ERR_SN_24_1
#
./trireme -s 1 -b 2 >SN_1_2 2>ERR_SN_1_2
./trireme -s 4 -b 2 >SN_4_2 2>ERR_SN_4_2
./trireme -s 8 -b 2 >SN_8_2 2>ERR_SN_8_2
./trireme -s 12 -b 2 >SN_12_2 2>ERR_SN_12_2
./trireme -s 16 -b 2 >SN_16_2 2>ERR_SN_16_2
./trireme -s 20 -b 2 >SN_20_2 2>ERR_SN_20_2
./trireme -s 24 -b 2 >SN_24_2 2>ERR_SN_24_2
#
./trireme -s 1 -b 2 >SN_1_3 2>ERR_SN_1_3
./trireme -s 4 -b 2 >SN_4_3 2>ERR_SN_4_3
./trireme -s 8 -b 2 >SN_8_3 2>ERR_SN_8_3
./trireme -s 12 -b 2 >SN_12_3 2>ERR_SN_12_3
./trireme -s 16 -b 2 >SN_16_3 2>ERR_SN_16_3
./trireme -s 20 -b 2 >SN_20_3 2>ERR_SN_20_3
./trireme -s 24 -b 2 >SN_24_3 2>ERR_SN_24_3
