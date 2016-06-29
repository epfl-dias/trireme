b=1
#for b in {1,2,4,8,16}; do
  for i in {1,2,3}; do 
    echo ./ycsb -s 64 -d 0 -b $b -i 10000000 -o 10 -t 64000000 -w 100 -a 100 -h 1 -p 16 >> skew-scaling.out 2>&1; 
    ./ycsb -s 64 -d 0 -b $b -i 10000000 -o 10 -t 64000000 -w 100 -a 100 -h 1 -p 16 >> skew-scaling.out 2>&1; 
  done

  for i in {1,2,3}; do 
    echo ./ycsb -s 64 -d 0 -b $b -i 10000000 -o 10 -t 64000000 -w 0 -a 100 -h 1 -p 16 >> skew-scaling-wt.out 2>&1; 
    ./ycsb -s 64 -d 0 -b $b -i 10000000 -o 10 -t 64000000 -w 0 -a 100 -h 1 -p 16 >> skew-scaling-wt.out 2>&1; 
  done

  for i in {1,2,3}; do 
    echo ./ycsb -s 64 -d 0 -b $b -i 10000000 -o 10 -t 64000000 -w 100 -a 100 -h 1 -p 32 >> skew-scaling.out 2>&1; 
    ./ycsb -s 64 -d 0 -b $b -i 10000000 -o 10 -t 64000000 -w 100 -a 100 -h 1 -p 32 >> skew-scaling.out 2>&1; 
  done

  for i in {1,2,3}; do 
    echo ./ycsb -s 64 -d 0 -b $b -i 10000000 -o 10 -t 64000000 -w 0 -a 100 -h 1 -p 32 >> skew-scaling-wt.out 2>&1; 
    ./ycsb -s 64 -d 0 -b $b -i 10000000 -o 10 -t 64000000 -w 0 -a 100 -h 1 -p 32 >> skew-scaling-wt.out 2>&1; 
  done

  for i in {1,2,4,8,16,32,64}; do
    for j in {1,2,3}; do 
      echo ./ycsb -s 64 -d 0 -b $b -i 10000000 -o 10 -t 64000000 -w 100 -a 100 -h $i -p 64 >> skew-scaling.out 2>&1; 
      ./ycsb -s 64 -d 0 -b $b -i 10000000 -o 10 -t 64000000 -w 100 -a 100 -h $i -p 64 >> skew-scaling.out 2>&1; 
    done
  done

  for i in {1,2,4,8,16,32,64}; do
    for j in {1,2,3}; do 
      echo ./ycsb -s 64 -d 0 -b $b -i 10000000 -o 10 -t 64000000 -w 0 -a 100 -p 64 -h $i >> skew-scaling-wt.out 2>&1; 
      ./ycsb -s 64 -d 0 -b $b -i 10000000 -o 10 -t 64000000 -w 0 -a 100 -p 64 -h $i >> skew-scaling-wt.out 2>&1; 
    done;
  done;

  #1 in 16
  for i in {1,2,3}; do 
    echo ./ycsb -s 64 -d 0 -b $b -i 10000000 -o 10 -t 64000000 -w 0 -a 100 -h 1 -p 16 >> skew-scaling-wt-imbal.out 2>&1;
    ./ycsb -s 64 -d 0 -b $b -i 10000000 -o 10 -t 64000000 -w 0 -a 100 -h 1 -p 16 >> skew-scaling-wt-imbal.out 2>&1; 
  done

  #2 in 8
  for i in {1,2,3}; do 
    echo ./ycsb -s 64 -d 0 -b $b -i 10000000 -o 10 -t 64000000 -w 0 -a 100 -h 2 -p 8 >> skew-scaling-wt-imbal.out 2>&1; 
    ./ycsb -s 64 -d 0 -b $b -i 10000000 -o 10 -t 64000000 -w 0 -a 100 -h 2 -p 8 >> skew-scaling-wt-imbal.out 2>&1; 
  done

  #4 in 4
  for i in {1,2,3}; do 
    echo ./ycsb -s 64 -d 0 -b $b -i 10000000 -o 10 -t 64000000 -w 0 -a 100 -h 4 -p 4 >> skew-scaling-wt-imbal.out 2>&1; 
    ./ycsb -s 64 -d 0 -b $b -i 10000000 -o 10 -t 64000000 -w 0 -a 100 -h 4 -p 4 >> skew-scaling-wt-imbal.out 2>&1; 
  done

  #8 in 2
  for i in {1,2,3}; do 
    echo ./ycsb -s 64 -d 0 -b $b -i 10000000 -o 10 -t 64000000 -w 0 -a 100 -h 8 -p 2 >> skew-scaling-wt-imbal.out 2>&1; 
    ./ycsb -s 64 -d 0 -b $b -i 10000000 -o 10 -t 64000000 -w 0 -a 100 -h 8 -p 2 >> skew-scaling-wt-imbal.out 2>&1; 
  done

  #16 in 1
  for i in {1,2,3}; do 
    echo ./ycsb -s 64 -d 0 -b $b -i 10000000 -o 10 -t 64000000 -w 0 -a 100 -h 16 -p 1 >> skew-scaling-wt-imbal.out 2>&1;
    ./ycsb -s 64 -d 0 -b $b -i 10000000 -o 10 -t 64000000 -w 0 -a 100 -h 16 -p 1 >> skew-scaling-wt-imbal.out 2>&1; 
  done

#done

#for b in {1,2,4,8,16}; do
  #1 in 16
  for i in {1,2,3}; do 
    echo ./ycsb -s 64 -d 0 -b $b -i 10000000 -o 10 -t 64000000 -w 100 -a 100 -h 1 -p 16 >> skew-scaling-imbal.out 2>&1; 
    ./ycsb -s 64 -d 0 -b $b -i 10000000 -o 10 -t 64000000 -w 100 -a 100 -h 1 -p 16 >> skew-scaling-imbal.out 2>&1; 
  done

  #2 in 8
  for i in {1,2,3}; do 
    echo ./ycsb -s 64 -d 0 -b $b -i 10000000 -o 10 -t 64000000 -w 100 -a 100 -h 2 -p 8 >> skew-scaling-imbal.out 2>&1; 
    ./ycsb -s 64 -d 0 -b $b -i 10000000 -o 10 -t 64000000 -w 100 -a 100 -h 2 -p 8 >> skew-scaling-imbal.out 2>&1; 
  done

  #4 in 4
  for i in {1,2,3}; do 
    echo ./ycsb -s 64 -d 0 -b $b -i 10000000 -o 10 -t 64000000 -w 100 -a 100 -h 4 -p 4 >> skew-scaling-imbal.out 2>&1; 
    ./ycsb -s 64 -d 0 -b $b -i 10000000 -o 10 -t 64000000 -w 100 -a 100 -h 4 -p 4 >> skew-scaling-imbal.out 2>&1; 
  done

  #8 in 2
  for i in {1,2,3}; do 
    echo ./ycsb -s 64 -d 0 -b $b -i 10000000 -o 10 -t 64000000 -w 100 -a 100 -h 8 -p 2 >> skew-scaling-imbal.out 2>&1; 
    ./ycsb -s 64 -d 0 -b $b -i 10000000 -o 10 -t 64000000 -w 100 -a 100 -h 8 -p 2 >> skew-scaling-imbal.out 2>&1; 
  done

  #16 in 1
  for i in {1,2,3}; do 
    echo ./ycsb -s 64 -d 0 -b $b -i 10000000 -o 10 -t 64000000 -w 100 -a 100 -h 16 -p 1 >> skew-scaling-imbal.out 2>&1; 
    ./ycsb -s 64 -d 0 -b $b -i 10000000 -o 10 -t 64000000 -w 100 -a 100 -h 16 -p 1 >> skew-scaling-imbal.out 2>&1; 
  done

#done





