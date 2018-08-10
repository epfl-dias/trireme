#include <assert.h>
#include <malloc.h>
#include <stdlib.h>
#include <stdint.h>
#include <string.h>

#include "const.h"
#include "messages.h"
#include "ring_buffer.h"
#include "type.h"
#include "util.h"

#define EXTRA_ASSERTS 0

struct box {
  struct ring_buffer * in;
  struct ring_buffer * out;
}  __attribute__ ((aligned (CACHELINE)));

struct box_array {
  struct box *boxes;
} __attribute__ ((aligned (CACHELINE)));

static size_t clients;
static size_t servers;

msg_errors_t
msg_alloc(struct box_array ** boxes, const size_t max_clients,
	const size_t max_servers)
{
  clients = max_clients;
  servers = max_servers;

  assert(0 < clients);
  assert(0 < servers);
  assert(NULL != boxes);

  *boxes = memalign(CACHELINE, clients * sizeof(struct box_array));
  assert(0 == ((uintptr_t) *boxes % CACHELINE));

  for (size_t i = 0; i < clients; i++) {
    (*boxes)[i].boxes = memalign(CACHELINE, servers * sizeof(struct box));
    assert(0 == ((uintptr_t) (*boxes)[i].boxes % CACHELINE));

    for (size_t j = 0; j < servers; j++) {
      memset(&(*boxes)[i].boxes[j], 0, sizeof(struct box));
      ring_buffer_alloc(&(*boxes)[i].boxes[j].in);
      ring_buffer_alloc(&(*boxes)[i].boxes[j].out);
    }
  }
  return 0;
}

msg_errors_t
msg_free(struct box_array ** boxes)
{
  assert(0 < clients);
  assert(0 < servers);
  assert(NULL != boxes);
  assert(NULL != *boxes);

  for (int i = 0; i < clients; i++) {
    for (int j = 0; j < servers; j++) {
      ring_buffer_free(&(*boxes)[i].boxes[j].in);
      ring_buffer_free(&(*boxes)[i].boxes[j].out);
    }
    free((*boxes)[i].boxes);
  }

  free(*boxes);

  *boxes = NULL;

  return 0;
}

msg_errors_t
msg_flush(struct box_array * boxes, const size_t client,
	const size_t server, uint32_t flags)
{
#if EXTRA_ASSERTS
  assert(NULL != boxes);
  assert(0 <= client);
  assert(clients > client);
  assert(0 <= server);
  assert(servers > server);
#endif

  if (MSG_IN == (flags & MSG_IN)) {
    ring_buffer_flush(boxes[client].boxes[server].in);
  }

  if (MSG_OUT == (flags & MSG_OUT)) {
    ring_buffer_flush(boxes[client].boxes[server].out);
  }

  return 0;
}

msg_errors_t
msg_send(struct box_array * boxes, const size_t client,
	const size_t server, uint32_t flags, const size_t count,
	const uint64_t * data)
{
#if EXTRA_ASSERTS
  assert(NULL != boxes);
  assert(0 <= client);
  assert(clients > client);
  assert(0 <= server);
  assert(servers > server);
  assert((MSG_IN | MSG_OUT) != (flags & (MSG_IN | MSG_OUT)));
#endif

  uint32_t flush = (MSG_FLUSH == (flags & MSG_FLUSH));

  if (MSG_IN == (flags & MSG_IN)) {
    ring_buffer_write_all(boxes[client].boxes[server].in, count, data, flush);
  }

  if (MSG_OUT == (flags & MSG_OUT)) {
    ring_buffer_write_all(boxes[client].boxes[server].out, count, data, flush);
  }
  dprint("SENDING from client %zu, srv %zu, MSG_%s, %zu messages\n", client, server,
  	(MSG_OUT == (flags & MSG_OUT))? "OUT" : "IN", count);

  return 0;
}

msg_errors_t
msg_receive(struct box_array * boxes, const size_t client,
	const size_t server, uint32_t flags, size_t * count,
	uint64_t * data)
{
#if EXTRA_ASSERTS
  assert(NULL != boxes);
  assert(0 <= client);
  assert(clients > client);
  assert(0 <= server);
  assert(servers > server);
  assert(NULL != count);
  assert((MSG_IN | MSG_OUT) != (flags & (MSG_IN | MSG_OUT)));
#endif

  uint32_t blocking = (MSG_BLOCK == (flags & MSG_BLOCK));
  size_t max = *count;

  if (MSG_IN == (flags & MSG_IN)) {
    *count = ring_buffer_read_all(boxes[client].boxes[server].in, max, data, blocking);
  }

  if (MSG_OUT == (flags & MSG_OUT)) {
    *count = ring_buffer_read_all(boxes[client].boxes[server].out, max, data, blocking);
  }

  dprint("RECEIVING client %zu, srv %zu, MSG_%s, %zu(%zu) messages\n", client, server,
  	(MSG_OUT == (flags & MSG_OUT))? "OUT" : "IN", *count, max);

  return 0;
}
