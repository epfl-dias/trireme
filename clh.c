#include "headers.h"

#if CLH_LOCK

volatile clh_qnode* clh_acquire(clh_lock *L, clh_qnode* I)
{
    assert(L && I);

    I->locked=1;

    clh_qnode_ptr pred = (clh_qnode*) swap_pointer((volatile void*) (L), (void*) I);

    if (!pred)       /* lock was free */
        return NULL;

    while (pred->locked != 0)
    {
        //_mm_pause();
    }

    return pred;
}

clh_qnode* clh_release(clh_qnode *my_qnode, clh_qnode * my_pred) {
    COMPILER_BARRIER();
    my_qnode->locked = 0;
    return my_pred;
}

#endif
