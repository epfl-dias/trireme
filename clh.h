#pragma once

volatile clh_qnode* clh_acquire(clh_lock *L, clh_qnode* I);
clh_qnode* clh_release(clh_qnode *my_qnode, clh_qnode * my_pred);
