DBGFLAGS="-DYCSB_BENCHMARK"
make clean
make "DFLAGS=-DENABLE_DL_DETECT_CC $DBGFLAGS"
./trireme -s 1 -b 1 -w 0 -a 0.9 -d 0 -o 16 > RO_09_ycsb_1_1.txt
./trireme -s 4 -b 1 -w 0 -a 0.9 -d 0 -o 16 > RO_09_ycsb_4_1.txt
./trireme -s 8 -b 1 -w 0 -a 0.9 -d 0 -o 16 > RO_09_ycsb_8_1.txt
./trireme -s 12 -b 1 -w 0 -a 0.9 -d 0 -o 16 > RO_09_ycsb_12_1.txt
./trireme -s 16 -b 1 -w 0 -a 0.9 -d 0 -o 16 > RO_09_ycsb_16_1.txt
./trireme -s 20 -b 1 -w 0 -a 0.9 -d 0 -o 16 > RO_09_ycsb_20_1.txt
./trireme -s 24 -b 1 -w 0 -a 0.9 -d 0 -o 16 > RO_09_ycsb_24_1.txt
./trireme -s 1 -b 1 -w 0 -a 0.9 -d 0 -o 16 > RO_09_ycsb_1_2.txt
./trireme -s 4 -b 1 -w 0 -a 0.9 -d 0 -o 16 > RO_09_ycsb_4_2.txt
./trireme -s 8 -b 1 -w 0 -a 0.9 -d 0 -o 16 > RO_09_ycsb_8_2.txt
./trireme -s 12 -b 1 -w 0 -a 0.9 -d 0 -o 16 > RO_09_ycsb_12_2.txt
./trireme -s 16 -b 1 -w 0 -a 0.9 -d 0 -o 16 > RO_09_ycsb_16_2.txt
./trireme -s 20 -b 1 -w 0 -a 0.9 -d 0 -o 16 > RO_09_ycsb_20_2.txt
./trireme -s 24 -b 1 -w 0 -a 0.9 -d 0 -o 16 > RO_09_ycsb_24_2.txt
./trireme -s 1 -b 1 -w 0 -a 0.9 -d 0 -o 16 > RO_09_ycsb_1_3.txt
./trireme -s 4 -b 1 -w 0 -a 0.9 -d 0 -o 16 > RO_09_ycsb_4_3.txt
./trireme -s 8 -b 1 -w 0 -a 0.9 -d 0 -o 16 > RO_09_ycsb_8_3.txt
./trireme -s 12 -b 1 -w 0 -a 0.9 -d 0 -o 16 > RO_09_ycsb_12_3.txt
./trireme -s 16 -b 1 -w 0 -a 0.9 -d 0 -o 16 > RO_09_ycsb_16_3.txt
./trireme -s 20 -b 1 -w 0 -a 0.9 -d 0 -o 16 > RO_09_ycsb_20_3.txt
./trireme -s 24 -b 1 -w 0 -a 0.9 -d 0 -o 16 > RO_09_ycsb_24_3.txt
make clean
make "DFLAGS=-DENABLE_DL_DETECT_CC $DBGFLAGS"
./trireme -s 1 -b 1 -w 1 -a 0.9 -d 0 -o 16 > WO_09_ycsb_1_1.txt
./trireme -s 4 -b 1 -w 4 -a 0.9 -d 0 -o 16 > WO_09_ycsb_4_1.txt
./trireme -s 8 -b 1 -w 8 -a 0.9 -d 0 -o 16 > WO_09_ycsb_8_1.txt
./trireme -s 12 -b 1 -w 12 -a 0.9 -d 0 -o 16 > WO_09_ycsb_12_1.txt
./trireme -s 16 -b 1 -w 16 -a 0.9 -d 0 -o 16 > WO_09_ycsb_16_1.txt
./trireme -s 20 -b 1 -w 20 -a 0.9 -d 0 -o 16 > WO_09_ycsb_20_1.txt
./trireme -s 24 -b 1 -w 24 -a 0.9 -d 0 -o 16 > WO_09_ycsb_24_1.txt
./trireme -s 1 -b 1 -w 1 -a 0.9 -d 0 -o 16 > WO_09_ycsb_1_2.txt
./trireme -s 4 -b 1 -w 4 -a 0.9 -d 0 -o 16 > WO_09_ycsb_4_2.txt
./trireme -s 8 -b 1 -w 8 -a 0.9 -d 0 -o 16 > WO_09_ycsb_8_2.txt
./trireme -s 12 -b 1 -w 12 -a 0.9 -d 0 -o 16 > WO_09_ycsb_12_2.txt
./trireme -s 16 -b 1 -w 16 -a 0.9 -d 0 -o 16 > WO_09_ycsb_16_2.txt
./trireme -s 20 -b 1 -w 20 -a 0.9 -d 0 -o 16 > WO_09_ycsb_20_2.txt
./trireme -s 24 -b 1 -w 24 -a 0.9 -d 0 -o 16 > WO_09_ycsb_24_2.txt
./trireme -s 1 -b 1 -w 1 -a 0.9 -d 0 -o 16 > WO_09_ycsb_1_3.txt
./trireme -s 4 -b 1 -w 4 -a 0.9 -d 0 -o 16 > WO_09_ycsb_4_3.txt
./trireme -s 8 -b 1 -w 8 -a 0.9 -d 0 -o 16 > WO_09_ycsb_8_3.txt
./trireme -s 12 -b 1 -w 12 -a 0.9 -d 0 -o 16 > WO_09_ycsb_12_3.txt
./trireme -s 16 -b 1 -w 16 -a 0.9 -d 0 -o 16 > WO_09_ycsb_16_3.txt
./trireme -s 20 -b 1 -w 20 -a 0.9 -d 0 -o 16 > WO_09_ycsb_20_3.txt
./trireme -s 24 -b 1 -w 24 -a 0.9 -d 0 -o 16 > WO_09_ycsb_24_3.txt

make clean
make "DFLAGS=-DENABLE_DL_DETECT_CC $DBGFLAGS"
./trireme -s 1 -b 1 -w 0 -a 0.1 -d 0 -o 16 > RO_01_ycsb_1_1.txt
./trireme -s 4 -b 1 -w 0 -a 0.1 -d 0 -o 16 > RO_01_ycsb_4_1.txt
./trireme -s 8 -b 1 -w 0 -a 0.1 -d 0 -o 16 > RO_01_ycsb_8_1.txt
./trireme -s 12 -b 1 -w 0 -a 0.1 -d 0 -o 16 > RO_01_ycsb_12_1.txt
./trireme -s 16 -b 1 -w 0 -a 0.1 -d 0 -o 16 > RO_01_ycsb_16_1.txt
./trireme -s 20 -b 1 -w 0 -a 0.1 -d 0 -o 16 > RO_01_ycsb_20_1.txt
./trireme -s 24 -b 1 -w 0 -a 0.1 -d 0 -o 16 > RO_01_ycsb_24_1.txt
./trireme -s 1 -b 1 -w 0 -a 0.1 -d 0 -o 16 > RO_01_ycsb_1_2.txt
./trireme -s 4 -b 1 -w 0 -a 0.1 -d 0 -o 16 > RO_01_ycsb_4_2.txt
./trireme -s 8 -b 1 -w 0 -a 0.1 -d 0 -o 16 > RO_01_ycsb_8_2.txt
./trireme -s 12 -b 1 -w 0 -a 0.1 -d 0 -o 16 > RO_01_ycsb_12_2.txt
./trireme -s 16 -b 1 -w 0 -a 0.1 -d 0 -o 16 > RO_01_ycsb_16_2.txt
./trireme -s 20 -b 1 -w 0 -a 0.1 -d 0 -o 16 > RO_01_ycsb_20_2.txt
./trireme -s 24 -b 1 -w 0 -a 0.1 -d 0 -o 16 > RO_01_ycsb_24_2.txt
./trireme -s 1 -b 1 -w 0 -a 0.1 -d 0 -o 16 > RO_01_ycsb_1_3.txt
./trireme -s 4 -b 1 -w 0 -a 0.1 -d 0 -o 16 > RO_01_ycsb_4_3.txt
./trireme -s 8 -b 1 -w 0 -a 0.1 -d 0 -o 16 > RO_01_ycsb_8_3.txt
./trireme -s 12 -b 1 -w 0 -a 0.1 -d 0 -o 16 > RO_01_ycsb_12_3.txt
./trireme -s 16 -b 1 -w 0 -a 0.1 -d 0 -o 16 > RO_01_ycsb_16_3.txt
./trireme -s 20 -b 1 -w 0 -a 0.1 -d 0 -o 16 > RO_01_ycsb_20_3.txt
./trireme -s 24 -b 1 -w 0 -a 0.1 -d 0 -o 16 > RO_01_ycsb_24_3.txt
make clean
make "DFLAGS=-DENABLE_DL_DETECT_CC $DBGFLAGS"
./trireme -s 1 -b 1 -w 1 -a 0.1 -d 0 -o 16 > WO_01_ycsb_1_1.txt
./trireme -s 4 -b 1 -w 4 -a 0.1 -d 0 -o 16 > WO_01_ycsb_4_1.txt
./trireme -s 8 -b 1 -w 8 -a 0.1 -d 0 -o 16 > WO_01_ycsb_8_1.txt
./trireme -s 12 -b 1 -w 12 -a 0.1 -d 0 -o 16 > WO_01_ycsb_12_1.txt
./trireme -s 16 -b 1 -w 16 -a 0.1 -d 0 -o 16 > WO_01_ycsb_16_1.txt
./trireme -s 20 -b 1 -w 20 -a 0.1 -d 0 -o 16 > WO_01_ycsb_20_1.txt
./trireme -s 24 -b 1 -w 24 -a 0.1 -d 0 -o 16 > WO_01_ycsb_24_1.txt
./trireme -s 1 -b 1 -w 1 -a 0.1 -d 0 -o 16 > WO_01_ycsb_1_2.txt
./trireme -s 4 -b 1 -w 4 -a 0.1 -d 0 -o 16 > WO_01_ycsb_4_2.txt
./trireme -s 8 -b 1 -w 8 -a 0.1 -d 0 -o 16 > WO_01_ycsb_8_2.txt
./trireme -s 12 -b 1 -w 12 -a 0.1 -d 0 -o 16 > WO_01_ycsb_12_2.txt
./trireme -s 16 -b 1 -w 16 -a 0.1 -d 0 -o 16 > WO_01_ycsb_16_2.txt
./trireme -s 20 -b 1 -w 20 -a 0.1 -d 0 -o 16 > WO_01_ycsb_20_2.txt
./trireme -s 24 -b 1 -w 24 -a 0.1 -d 0 -o 16 > WO_01_ycsb_24_2.txt
./trireme -s 1 -b 1 -w 1 -a 0.1 -d 0 -o 16 > WO_01_ycsb_1_3.txt
./trireme -s 4 -b 1 -w 4 -a 0.1 -d 0 -o 16 > WO_01_ycsb_4_3.txt
./trireme -s 8 -b 1 -w 8 -a 0.1 -d 0 -o 16 > WO_01_ycsb_8_3.txt
./trireme -s 12 -b 1 -w 12 -a 0.1 -d 0 -o 16 > WO_01_ycsb_12_3.txt
./trireme -s 16 -b 1 -w 16 -a 0.1 -d 0 -o 16 > WO_01_ycsb_16_3.txt
./trireme -s 20 -b 1 -w 20 -a 0.1 -d 0 -o 16 > WO_01_ycsb_20_3.txt
./trireme -s 24 -b 1 -w 24 -a 0.1 -d 0 -o 16 > WO_01_ycsb_24_3.txt
