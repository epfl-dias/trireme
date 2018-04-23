#se_no_wait
make clean
make "DFLAGS=-DSHARED_EVERYTHING -DENABLE_NOWAIT_CC -DSE_LATCH -DSE_INDEX_LATCH -DPTHREAD_SPINLOCK "
./trireme -s 1 -b 2 >SE_NW_1_1 2>ERR_SE_NW_1_1
./trireme -s 4 -b 2 >SE_NW_4_1 2>ERR_SE_NW_4_1
./trireme -s 8 -b 2 >SE_NW_8_1 2>ERR_SE_NW_8_1
./trireme -s 12 -b 2 >SE_NW_12_1 2>ERR_SE_NW_12_1
./trireme -s 16 -b 2 >SE_NW_16_1 2>ERR_SE_NW_16_1
./trireme -s 20 -b 2 >SE_NW_20_1 2>ERR_SE_NW_20_1
./trireme -s 24 -b 2 >SE_NW_24_1 2>ERR_SE_NW_24_1
#
./trireme -s 1 -b 2 >SE_NW_1_2 2>ERR_SE_NW_1_2
./trireme -s 4 -b 2 >SE_NW_4_2 2>ERR_SE_NW_4_2
./trireme -s 8 -b 2 >SE_NW_8_2 2>ERR_SE_NW_8_2
./trireme -s 12 -b 2 >SE_NW_12_2 2>ERR_SE_NW_12_2
./trireme -s 16 -b 2 >SE_NW_16_2 2>ERR_SE_NW_16_2
./trireme -s 20 -b 2 >SE_NW_20_2 2>ERR_SE_NW_20_2
./trireme -s 24 -b 2 >SE_NW_24_2 2>ERR_SE_NW_24_2
#
./trireme -s 1 -b 2 >SE_NW_1_3 2>ERR_SE_NW_1_3
./trireme -s 4 -b 2 >SE_NW_4_3 2>ERR_SE_NW_4_3
./trireme -s 8 -b 2 >SE_NW_8_3 2>ERR_SE_NW_8_3
./trireme -s 12 -b 2 >SE_NW_12_3 2>ERR_SE_NW_12_3
./trireme -s 16 -b 2 >SE_NW_16_3 2>ERR_SE_NW_16_3
./trireme -s 20 -b 2 >SE_NW_20_3 2>ERR_SE_NW_20_3
./trireme -s 24 -b 2 >SE_NW_24_3 2>ERR_SE_NW_24_3
#
#se_dl_detect
make clean
make "DFLAGS=-DSHARED_EVERYTHING -DENABLE_DL_DETECT_CC -DSE_LATCH -DSE_INDEX_LATCH -DPTHREAD_SPINLOCK "
./trireme -s 1 -b 2 >SE_DL_1_1 2>ERR_SE_DL_1_1
./trireme -s 4 -b 2 >SE_DL_4_1 2>ERR_SE_DL_4_1
./trireme -s 8 -b 2 >SE_DL_8_1 2>ERR_SE_DL_8_1
./trireme -s 12 -b 2 >SE_DL_12_1 2>ERR_SE_DL_12_1
./trireme -s 16 -b 2 >SE_DL_16_1 2>ERR_SE_DL_16_1
./trireme -s 20 -b 2 >SE_DL_20_1 2>ERR_SE_DL_20_1
./trireme -s 24 -b 2 >SE_DL_24_1 2>ERR_SE_DL_24_1
#
./trireme -s 1 -b 2 >SE_DL_1_2 2>ERR_SE_DL_1_2
./trireme -s 4 -b 2 >SE_DL_4_2 2>ERR_SE_DL_4_2
./trireme -s 8 -b 2 >SE_DL_8_2 2>ERR_SE_DL_8_2
./trireme -s 12 -b 2 >SE_DL_12_2 2>ERR_SE_DL_12_2
./trireme -s 16 -b 2 >SE_DL_16_2 2>ERR_SE_DL_16_2
./trireme -s 20 -b 2 >SE_DL_20_2 2>ERR_SE_DL_20_2
./trireme -s 24 -b 2 >SE_DL_24_2 2>ERR_SE_DL_24_2
#
./trireme -s 1 -b 2 >SE_DL_1_3 2>ERR_SE_DL_1_3
./trireme -s 4 -b 2 >SE_DL_4_3 2>ERR_SE_DL_4_3
./trireme -s 8 -b 2 >SE_DL_8_3 2>ERR_SE_DL_8_3
./trireme -s 12 -b 2 >SE_DL_12_3 2>ERR_SE_DL_12_3
./trireme -s 16 -b 2 >SE_DL_16_3 2>ERR_SE_DL_16_3
./trireme -s 20 -b 2 >SE_DL_20_3 2>ERR_SE_DL_20_3
./trireme -s 24 -b 2 >SE_DL_24_3 2>ERR_SE_DL_24_3