#ifndef __UTIL_H_
#define __UTIL_H_

#include <sys/types.h>
#include <stdint.h>
#include <limits.h>

#ifdef DEBUG
#define dprint(...) \
{\
  printf(__VA_ARGS__); \
  fflush(stdout);\
}
#else
#define dprint(...) 
#endif

#define CPU_FREQ 2 	// in GHz/s
#define NOGRAPHITE
#define TIME_ENABLE	true

/* bitarray macros */
#define BITMASK(b) (1 << ((b) % CHAR_BIT))
#define BITSLOT(b) ((b) / CHAR_BIT)
#define BITSET(a, b) ((a)[BITSLOT(b)] |= BITMASK(b))
#define BITTEST(a, b) ((a)[BITSLOT(b)] & BITMASK(b))
#define BITNSLOTS(nb) ((nb + CHAR_BIT - 1) / CHAR_BIT)

#define COMPILER_BARRIER() __asm__ __volatile__("" ::: "memory")

pid_t gettid(void);
void set_affinity(int cpu_id);
double now();

static inline void* swap_pointer(volatile void* ptr, void *x) {
    __asm__ __volatile__("xchgq %0,%1"
            :"=r" ((unsigned long long) x)
            :"m" (*(volatile long long *)ptr), "0" ((unsigned long long) x)
            :"memory");

    return x;
}

static inline int max(int a, int b) 
{ 
  if (a > b) return a; 
  else return b; 
}

static inline int min(int a, int b) 
{ 
  if (a < b) return a; 
  else return b; 
}

static inline unsigned xchg_32(void *ptr, unsigned x)
{
  __asm__ __volatile__("xchgl %0,%1"
        :"=r" ((unsigned) x)
        :"m" (*(volatile unsigned *)ptr), "0" (x)
        :"memory");

  return x;
}

static inline uint64_t __attribute__((always_inline))
read_tsc(void)
{
  uint32_t a, d;
  __asm __volatile("rdtsc" : "=a" (a), "=d" (d));
  return ((uint64_t) a) | (((uint64_t) d) << 32);
}


static inline void nop_rep(uint32_t num_reps)
{
    uint32_t i;
    for (i = 0; i < num_reps; i++)
    {
        __asm __volatile ("NOP");
    }
}

static inline uint64_t get_server_clock() {
#if defined(__i386__)
    uint64_t ret;
    __asm__ __volatile__("rdtsc" : "=A" (ret));
#elif defined(__x86_64__)
    unsigned hi, lo;
    __asm__ __volatile__ ("rdtsc" : "=a"(lo), "=d"(hi));
    uint64_t ret = ( (uint64_t)lo)|( ((uint64_t)hi)<<32 );
	ret = (uint64_t) ((double)ret / CPU_FREQ);
#else
	timespec * tp = new timespec;
    clock_gettime(CLOCK_REALTIME, tp);
    uint64_t ret = tp->tv_sec * 1000000000 + tp->tv_nsec;
#endif
    return ret;
}

static inline uint64_t get_sys_clock() {
  #if TIME_ENABLE
	return get_server_clock();
  #else
	return 0;
  #endif
}


uint64_t *zipf_get_keys(double alpha, uint64_t N, uint64_t nvalues);

int RAND(unsigned int *seed, int max);
int URand(unsigned int *seed, int x, int y);
int NURand(unsigned int *seed, int A, int x, int y);
int make_alpha_string(unsigned int *seed, int min, int max, char* str);
int make_numeric_string(unsigned int *seed, int min, int max, char* str);

#endif
