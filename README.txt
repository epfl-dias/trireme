- fix partition lock mode to work together with wait die

- shared everything
  - make sure its compiled with se_latch. 
  - make sure you use -t 20M for tpcc. otherwise, there are > 1 record per
    bucket. this causes bucket contention
