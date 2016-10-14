#include "headers.h"

#if HTLOCK

__thread uint32_t htlock_node_mine, htlock_id_mine;

int create_htlock(htlock_t* htl)
{
//    htlock_t* htl;
//    htl = memalign(CACHELINE, sizeof(htlock_t));
//    if (htl == NULL) 
//    {
//        fprintf(stderr,"Error @ memalign : create htlock\n");
//    }
    assert(htl != NULL);

    htl->global = memalign(CACHELINE, sizeof(htlock_global_t));
    if (htl == NULL) 
    {
        fprintf(stderr,"Error @ memalign : create htlock\n");
     }
    assert(htl->global != NULL);

    uint32_t s;
    for (s = 0; s < NUMBER_OF_SOCKETS; s++)
    {
        htl->local[s] = (htlock_local_t*) malloc(sizeof(htlock_local_t));
        htl->local[s]->cur = NB_TICKETS_LOCAL;
        htl->local[s]->nxt = 0;
        assert(htl->local != NULL);
    }

    htl->global->cur = 0;
    htl->global->nxt = 0;

    MEM_BARRIER;
    return 0;
}


    void
init_htlock(htlock_t* htl)
{
    assert(htl != NULL);
    htl->global->cur = 0;
    htl->global->nxt = 0;
    uint32_t n;
    for (n = 0; n < NUMBER_OF_SOCKETS; n++)
    {
        htl->local[n]->cur = NB_TICKETS_LOCAL;
        htl->local[n]->nxt = 0;
    }
    MEM_BARRIER;
}

    void
init_thread_htlocks(uint32_t phys_core)
{
    set_affinity(phys_core);

    uint32_t real_core_num = 0;
    uint32_t i;
    for (i = 0; i < NCORES; i++) 
    {
        if (the_cores[i]==phys_core) 
        {
            real_core_num = i;
            break;
        }
    }
    htlock_id_mine = real_core_num;
    htlock_node_mine = phys_core % 4;

    MEM_BARRIER;
}

    uint32_t
is_free_hticket(htlock_t* htl)
{
    htlock_global_t* glb = htl->global;
#if defined(OPTERON_OPTIMIZE)
    PREFETCHW(glb);
#endif
    if (glb->cur == glb->nxt) 
    {
        return 1;
    }
    return 0;
}

    static htlock_t* 
create_htlock_no_alloc(htlock_t* htl, htlock_local_t* locals[NUMBER_OF_SOCKETS], size_t offset)
{
    htl->global = memalign(CACHELINE, sizeof(htlock_global_t));
    if (htl == NULL) 
    {
        fprintf(stderr,"Error @ memalign : create htlock\n");
    }
    assert(htl->global != NULL);

    uint32_t s;
    for (s = 0; s < NUMBER_OF_SOCKETS; s++)
    {
        htl->local[s] = locals[s] + offset;
    }

    htl->global->cur = 0;
    htl->global->nxt = 0;
    uint32_t n;
    for (n = 0; n < NUMBER_OF_SOCKETS; n++)
    {
        htl->local[n]->cur = NB_TICKETS_LOCAL;
        htl->local[n]->nxt = 0;
    }

    MEM_BARRIER;
    return htl;
}

    htlock_t*
init_htlocks(uint32_t num_locks)
{
    htlock_t* htls;
    htls = memalign(CACHELINE, num_locks * sizeof(htlock_t));
    if (htls == NULL) 
    {
        fprintf(stderr, "Error @ memalign : init_htlocks\n");
    }
    assert(htls != NULL);


    size_t alloc_locks = (num_locks < 64) ? 64 : num_locks;

    htlock_local_t* locals[NUMBER_OF_SOCKETS];
    uint32_t n;
    for (n = 0; n < NUMBER_OF_SOCKETS; n++)
    {
        locals[n] = (htlock_local_t*) calloc(alloc_locks, sizeof(htlock_local_t));
        *((volatile int*) locals[n]) = 33;
        assert(locals[n] != NULL);
    }

    uint32_t i;
    for (i = 0; i < num_locks; i++)
    {
        create_htlock_no_alloc(htls + i, locals, i);
    }

    MEM_BARRIER;
    return htls;
}


    void 
free_htlocks(htlock_t* locks)
{
    free(locks);
}

    static inline uint32_t
sub_abs(const uint32_t a, const uint32_t b)
{
    if (a > b)
    {
        return a - b;
    }
    else
    {
        return b - a;
    }
}


#define TICKET_BASE_WAIT 512
#define TICKET_MAX_WAIT  4095
#define TICKET_WAIT_NEXT 64

static inline void
htlock_wait_ticket(htlock_local_t* lock, const uint32_t ticket)
{
    while (lock->cur != ticket)
    {
        uint32_t distance = sub_abs(lock->cur, ticket);
        if (distance > 1)
        {
            nop_rep(distance * TICKET_BASE_WAIT);
        }
        else
        {
            _mm_pause();
        }
    }
}

    static inline void
htlock_wait_global(htlock_local_t* lock, const uint32_t ticket)
{
    while (lock->cur != ticket)
    {
        uint32_t distance = sub_abs(lock->cur, ticket);
        if (distance > 1)
        {
            wait_cycles(distance * 256);
        }
        else
        {
            _mm_pause();
        }
    }
}

    void
htlock_lock(htlock_t* l)
{
    htlock_local_t* localp = l->local[htlock_node_mine];
    int32_t local_ticket;

again_local:
    local_ticket = DAF_U32(&localp->nxt);
    if (local_ticket < -1)	
    {
        _mm_pause();
        wait_cycles(-local_ticket * 120);
        _mm_pause();
        goto again_local;
    }

    if (local_ticket >= 0)	/* local grabing successful */
    {
        htlock_wait_ticket((htlock_local_t*) localp, local_ticket);
    }
    else				/* no local ticket available */
    {
        do
        {
        } while (localp->cur != NB_TICKETS_LOCAL);
        localp->nxt = NB_TICKETS_LOCAL; /* give tickets to the local neighbors */

        htlock_global_t* globalp = l->global;
        uint32_t global_ticket = FAI_U32(&globalp->nxt);

        htlock_wait_global((htlock_local_t*) globalp, global_ticket);
    }
}

    void
htlock_release(htlock_t* l)
{
    htlock_local_t* localp = l->local[htlock_node_mine];
    int32_t local_cur = localp->cur;
    int32_t local_nxt = CAS_U32(&localp->nxt, local_cur, 0);
    if (local_cur == 0 || local_cur == local_nxt) /* global */
    {
#if defined(OPTERON_OPTIMIZE)
        PREFETCHW((l->global));
        PREFETCHW(localp);
#endif
        localp->cur = NB_TICKETS_LOCAL;
        l->global->cur++;
    }
    else				/* local */
    {
#if defined(OPTERON_OPTIMIZE)
        PREFETCHW(localp);
#endif
        localp->cur = local_cur - 1;
    }
}

    uint32_t 
htlock_trylock(htlock_t* l)
{
    htlock_global_t* globalp = l->global;
    uint32_t global_nxt = globalp->nxt;

    htlock_global_t tmp = 
    {
        .nxt = global_nxt, 
        .cur = global_nxt
    };
    htlock_global_t tmp_new = 
    {
        .nxt = global_nxt + 1, 
        .cur = global_nxt
    };

    uint64_t tmp64 = *(uint64_t*) &tmp;
    uint64_t tmp_new64 = *(uint64_t*) &tmp_new;

    if (CAS_U64((uint64_t*) globalp, tmp64, tmp_new64) == tmp64)
    {
        return 1;
    }

    return 0;
}


    inline void
htlock_release_try(htlock_t* l)	/* trylock rls */
{
    l->global->cur++;
}

#endif
