#pragma once

int no_wait_check_acquire(struct elem *e, char optype);
int no_wait_acquire(struct elem *e, char optype);
void no_wait_release(struct partition *p, struct elem *e);

int wait_die_acquire(int s, struct partition *p,
    int c, int task_id, int op_id, struct elem *e, char optype,
    uint64_t req_ts, struct lock_entry **pl);
void wait_die_release(int s, struct partition *p, int c, int task_id,
    int op_id, struct elem *e);
int wait_die_check_acquire(int s, struct partition *p,
    int c, int tid, int opid, struct elem *e, char optype, uint64_t req_ts);

int bwait_acquire(int s, struct partition *p,
    int c, int task_id, int op_id, struct elem *e, char optype,
    struct lock_entry **pl);
void bwait_release(int s, struct partition *p, int c, int task_id,
    int op_id, struct elem *e);
int bwait_check_acquire(struct elem *e, char optype);

int dl_detect_acquire(int s, struct partition *p,
    int c, int task_id, int op_id, struct elem *e, char optype,
    struct lock_entry **pl, uint64_t ts, int *notification);
void dl_detect_release(int s, struct partition *p, int c, int task_id,
    int op_id, struct elem *e, int notify);
int dl_detect_check_acquire(struct elem *e, char optype);
