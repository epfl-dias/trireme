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
#include <limits.h>
#include <sys/queue.h>
#include <stdbool.h>
#include <stdint.h>
#include "pthread.h"

#include <ucontext.h>
#include "const.h"
#include "hashprotocol.h"
#include "type.h"
#include "glo.h"
#include "util.h"
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
#elif DRW_LOCK
#include "drwlock.h"
#elif CLH_LOCK
#include "clh.h"
#endif

#endif

