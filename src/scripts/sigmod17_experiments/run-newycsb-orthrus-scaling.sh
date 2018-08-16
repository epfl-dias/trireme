#!/bin/bash
cd ../..

make clean
make "DFLAGS=-DENABLE_ASYMMETRIC_MESSAGING "

threads=(1 2 4 8 18 36 54 72)
for t in ${threads[@]}; do
    ns=`expr $t / 2`
    i=0
    s=${threads[$i]}
    echo $i - $s - $ns
    while (($s <= $ns)); do
        echo $i - $s - $ns
        for l in `seq 1 3`; do
#            echo ./ycsb -s $t -d 0 -b 8 -i 100000000 -o 10 -r 10 -t 72 -w 100 -a 100 -h `expr 72 / $s` -p $s

#            echo ./ycsb -s $t -d 0 -b 8 -i 100000000 -o 10 -r 10 -t 72 -w 100 -a 100 -h `expr 72 / $s` -p $s >> tr.orthrus.asymmetric.client.server.rdscaling.out

#            ./ycsb -s $t -d 0 -b 8 -i 100000000 -o 10 -r 10 -t 72 -w 100 -a 100 -h `expr 72 / $s` -p $s >> tr.orthrus.asymmetric.client.server.rdscaling.out

            echo ./ycsb -s $t -d 0 -b 8 -i 100000000 -o 10 -r 10 -t 72 -w 0 -a 100 -h `expr 72 / $s` -p $s

            echo ./ycsb -s $t -d 0 -b 8 -i 100000000 -o 10 -r 10 -t 72 -w 0 -a 100 -h `expr 72 / $s` -p $s >> tr.orthrus.asymmetric.client.server.wtscaling.out

            ./ycsb -s $t -d 0 -b 8 -i 100000000 -o 10 -r 10 -t 72 -w 0 -a 100 -h `expr 72 / $s` -p $s >> tr.orthrus.asymmetric.client.server.wtscaling.out

        done

        i=$((i+1))
        s=${threads[$i]}
    done
done

