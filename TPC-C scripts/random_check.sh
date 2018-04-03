make clean
make "DFLAGS=-DENABLE_DL_DETECT_CC $DBGFLAGS"
./trireme -s 1 -b 2
./trireme -s 4 -b 2
./trireme -s 8 -b 2
./trireme -s 12 -b 2
./trireme -s 16 -b 2
./trireme -s 20 -b 2
./trireme -s 24 -b 2
