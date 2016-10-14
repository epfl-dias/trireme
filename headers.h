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
#include <xmmintrin.h>

#include <ucontext.h>
#include "const.h"
#include "glo.h"
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
#elif HTLOCK
#include "htlock.h"
#elif RWTICKET_LOCK
#include "rwticket_lock.h"
#elif SIMPLE_SPINLOCK
#include "sspinlock.h"
#elif RW_LOCK
#include "rwlock.h"
#elif CLH_LOCK
#include "clh.h"
#endif

#endif

