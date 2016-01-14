- shared everything
  - make sure its compiled with se_latch. 
  - make sure you use -t 20M for tpcc. otherwise, there are > 1 record per
    bucket. this causes bucket contention
