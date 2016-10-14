#include <sched.h>
#include <sys/syscall.h>
#include <sys/time.h>
#include "headers.h"

pid_t gettid(void) 
{
  return syscall(__NR_gettid);
}

void set_affinity(int cpu_id)
{
  int tid = gettid();
  cpu_set_t mask;
  CPU_ZERO(&mask);
  CPU_SET(cpu_id, &mask);
  int r = sched_setaffinity(tid, sizeof(mask), &mask);
  if (r < 0) {
    fprintf(stderr, "couldn't set affinity for cpu_id:%d\n", cpu_id);
    exit(1);
  }
}

double now()
{
  struct timeval tv;
  gettimeofday(&tv, 0);
  return tv.tv_sec + tv.tv_usec / 1000000.0;
}

int RAND(unsigned int *seed, int max)
{ 
  return rand_r(seed) % max;
}

int URand(unsigned int *seed, int x, int y)
{
  return x + RAND(seed, y - x + 1);
}

int NURand(unsigned int *seed, int A, int x, int y)
{
  static char C_255_init = FALSE;
  static char C_1023_init = FALSE;
  static char C_8191_init = FALSE;
  static int C_255, C_1023, C_8191;
  int C = 0;
  switch(A) {
    case 255:
      if(!C_255_init) {
        C_255 = URand(seed, 0,255);
        C_255_init = TRUE;
      }
      C = C_255;
      break;
    case 1023:
      if(!C_1023_init) {
        C_1023 = URand(seed, 0,1023);
        C_1023_init = TRUE;
      }
      C = C_1023;
      break;
    case 8191:
      if(!C_8191_init) {
        C_8191 = URand(seed, 0,8191);
        C_8191_init = TRUE;
      }
      C = C_8191;
      break;
    default:
      assert(0);
      exit(-1);
  }
  return(((URand(seed, 0,A) | URand(seed, x,y))+C)%(y-x+1))+x;
}

int make_alpha_string(unsigned int *seed, int min, int max, char* str)
{
  char char_list[] = {'1','2','3','4','5','6','7','8','9','a','b','c',
    'd','e','f','g','h','i','j','k','l','m','n','o',
    'p','q','r','s','t','u','v','w','x','y','z','A',
    'B','C','D','E','F','G','H','I','J','K','L','M',
    'N','O','P','Q','R','S','T','U','V','W','X','Y','Z'};
  int cnt = URand(seed, min, max);
  for (uint32_t i = 0; i < cnt; i++)
    str[i] = char_list[URand(seed, 0L, 60L)];

  for (int i = cnt; i < max; i++)
    str[i] = '\0';

  return cnt;
}

int make_numeric_string(unsigned int *seed, int min, int max, char* str)
{
  int cnt = URand(seed, min, max);

  for (int i = 0; i < cnt; i++) {
    int r = URand(seed, 0L,9L);
    str[i] = '0' + r;
  }
  return cnt;
}

