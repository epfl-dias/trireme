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

#include <ucontext.h>
#include "const.h"
#include "hashprotocol.h"
#include "util.h"
#include "type.h"
#include "task.h"
#include "ucontext_i.h"

#ifdef ANDERSON_LOCK
#include "alock.h"
#elif TICKET_LOCK
#include "tlock.h"
#elif TAS_LOCK
#include "taslock.h"
#endif

#endif

