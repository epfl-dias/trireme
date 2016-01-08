/*
 *        Author: Kenneth J. Christensen
 *        University of South Florida
 *        WWW: http://www.csee.usf.edu/~christen
 *        Email: christen@csee.usf.edu
 */

#include <assert.h>             // Needed for assert() macro
#include <stdio.h>              // Needed for printf()
#include <stdlib.h>             // Needed for exit() and ato*()
#include <math.h>               // Needed for pow()
#include <stdint.h>
#include <inttypes.h>
#include "util.h"


static double *power_table = NULL;

//=========================================================================
//= Multiplicative LCG for generating uniform(0.0, 1.0) random numbers    =
//=   - x_n = 7^5*x_(n-1)mod(2^31 - 1)                                    =
//=   - With x seeded to 1 the 10000th x value should be 1043618065       =
//=   - From R. Jain, "The Art of Computer Systems Performance Analysis," =
//=     John Wiley & Sons, 1991. (Page 443, Figure 26.2)                  =
//=========================================================================
double rand_val(int seed)
{
  const long  a =      16807;  // Multiplier
  const long  m = 2147483647;  // Modulus
  const long  q =     127773;  // m div a
  const long  r =       2836;  // m mod a
  static long x;               // Random int value
  long        x_div_q;         // x divided by q
  long        x_mod_q;         // x modulo q
  long        x_new;           // New x value

  // Set the seed if argument is non-zero and then return zero
  if (seed > 0)
  {
    x = seed;
    return(0.0);
  }

  // RNG using integer arithmetic
  x_div_q = x / q;
  x_mod_q = x % q;
  x_new = (a * x_mod_q) - (r * x_div_q);
  if (x_new > 0)
    x = x_new;
  else
    x = x_new + m;

  // Return a random value between 0.0 and 1.0
  return((double) x / m);
}

int zipf(double alpha, uint64_t n)
{
  static int first = TRUE;      // Static first time flag
  static double c = 0;          // Normalization constant
  double z;                     // Uniform random number (0 < z < 1)
  double sum_prob;              // Sum of probabilities
  double zipf_value = 0;            // Computed exponential value to be returned
  uint64_t i;                     // Loop counter

  if (first == TRUE)
  {
    for (i=1; i<=n; i++)
      c = c + (1.0 / power_table[i]);
 
    c = 1.0 / c;

    first = FALSE;
  }

  // Pull a uniform random number (0 < z < 1)
  do
  {
    z = rand_val(0);
  } while ((z == 0) || (z == 1));

  // Map z to the value
  sum_prob = 0;
  for (i=1; i<=n; i++)
  {
    sum_prob = sum_prob + c / power_table[i];
    if (sum_prob >= z)
    {
      zipf_value = i;
      break;
    }
  }

  // Assert that zipf_value is between 1 and N
  assert((zipf_value >=1) && (zipf_value <= n));

  return(zipf_value);
}

uint64_t *zipf_get_keys(double alpha, uint64_t N, uint64_t nvalues)
{
  uint64_t *zipf_rv;
  uint64_t i;
  uint64_t seed = 19890811;
  int p = 10;

  rand_val(seed);

  printf("\nIniting power table: ");
  fflush(stdout);

  power_table = malloc(sizeof(double) * (N + 1));
  assert(power_table);

  for (i=1; i<=N; i++) {
    power_table[i] = pow((double) i, alpha);

    if (N * p / 100 == i) {
      printf("%d%% ", p);
      fflush(stdout);
      p += 10;
    }
  }

  printf("\nDone initing power table. Generating queries:\n");
  fflush(stdout);

  p = 10;
  zipf_rv = malloc(sizeof(uint64_t) * nvalues);
  assert(zipf_rv);

  // Generate and output zipf random variables
  for (i = 0; i < nvalues; i++) {
    zipf_rv[i] = zipf(alpha, N);  
    if (i == nvalues * p / 100) {
      printf("%d%%.. ", p);
      fflush(stdout);
      p+=10;
    }

 }

  printf("100%% \n");

  return zipf_rv;
}
