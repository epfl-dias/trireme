#ifndef __HEADERS_H__
#define __HEADERS_H__

#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <assert.h>
#include <string.h>
#include <sys/types.h>
#include <malloc.h>
#include <pthread.h>
#include <math.h>
#include <inttypes.h>

#include "const.h"
#include "type.h"
#include "util.h"

#ifdef ANDERSON_LOCK
#include "alock.h"
#elif TICKET_LOCK
#include "tlock.h"
#endif

#endif

