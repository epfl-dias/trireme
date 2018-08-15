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

#define MSG_QUEUES_MASK (MSG_MAX_QUEUES - 1U)

struct box {
  struct ring_buffer * queues[MSG_MAX_QUEUES];
};

struct mailbox {
  struct box *boxes;
};

static size_t clients;
static size_t servers;

msg_errors_t
msg_alloc(struct mailbox ** boxes, const size_t max_clients,
	const size_t max_servers)
{
  clients = max_clients;
  servers = max_servers;

  assert(0 < clients);
  assert(0 < servers);
  assert(NULL != boxes);

  *boxes = malloc(clients * sizeof(struct mailbox));

  for (size_t i = 0; i < clients; i++) {
    (*boxes)[i].boxes = malloc(servers * sizeof(struct box));

    for (size_t j = 0; j < servers; j++) {
      memset(&(*boxes)[i].boxes[j], 0, sizeof(struct box));
      for (size_t q = 0; q < MSG_MAX_QUEUES; q++) {
        ring_buffer_alloc(&(*boxes)[i].boxes[j].queues[q]);
      }
    }
  }

  return 0;
}

msg_errors_t
msg_free(struct mailbox ** boxes)
{
  assert(0 < clients);
  assert(0 < servers);
  assert(NULL != boxes);
  assert(NULL != *boxes);


  for (int i = 0; i < clients; i++) {
    for (int j = 0; j < servers; j++) {
      for (size_t q = 0; q < MSG_MAX_QUEUES; q++) {
        ring_buffer_free(&(*boxes)[i].boxes[j].queues[q]);
      }
    }
    free((*boxes)[i].boxes);
  }

  free(*boxes);

  *boxes = NULL;

  return 0;
}

msg_errors_t
msg_flush(struct mailbox * boxes, const size_t client, const size_t server,
	uint32_t flags)
{
#if EXTRA_ASSERTS
  assert(NULL != boxes);
  assert(0 <= client);
  assert(clients > client);
  assert(0 <= server);
  assert(servers > server);
#endif

  struct ring_buffer * b =
      boxes[client].boxes[server].queues[flags & MSG_QUEUES_MASK];

  ring_buffer_flush(b);

  return 0;
}

msg_errors_t
msg_send(struct mailbox * boxes, const size_t client, const size_t server,
	uint32_t flags, const size_t count, const uint64_t * data)
{
#if EXTRA_ASSERTS
  assert(NULL != boxes);
  assert(0 <= client);
  assert(clients > client);
  assert(0 <= server);
  assert(servers > server);
#endif

  uint32_t flush = (MSG_FLUSH == (flags & MSG_FLUSH));

  struct ring_buffer * b =
      boxes[client].boxes[server].queues[flags & MSG_QUEUES_MASK];

  ring_buffer_write_all(b, count, data, flush);

  dprint("SENDING from client %zu, srv %zu, MSG_%s, %zu messages\n", client,
  	server, (MSG_OUT == (flags & MSG_OUT))? "OUT" : "IN", count);

  return 0;
}

msg_errors_t
msg_receive(struct mailbox * boxes, const size_t client, const size_t server,
	uint32_t flags, size_t * count, uint64_t * data)
{
#if EXTRA_ASSERTS
  assert(NULL != boxes);
  assert(0 <= client);
  assert(clients > client);
  assert(0 <= server);
  assert(servers > server);
  assert(NULL != count);
#endif

  uint32_t blocking = (MSG_BLOCK == (flags & MSG_BLOCK));
  size_t max = *count;

  struct ring_buffer * b =
      boxes[client].boxes[server].queues[flags & MSG_QUEUES_MASK];

  *count = ring_buffer_read_all(b, max, data, blocking);

  dprint("RECEIVING client %zu, srv %zu, MSG_%s, %zu(%zu) messages\n", client,
  	server, (MSG_OUT == (flags & MSG_OUT))? "OUT" : "IN", *count, max);

  return 0;
}

inline msg_errors_t
msg_pending(struct mailbox * boxes, const size_t client, const size_t server,
	uint32_t flags, size_t * count)
{
#if EXTRA_ASSERTS
  assert(NULL != boxes);
  assert(0 <= client);
  assert(clients > client);
  assert(0 <= server);
  assert(servers > server);
  assert(NULL != count);
#endif

  struct ring_buffer * b =
      boxes[client].boxes[server].queues[flags & MSG_QUEUES_MASK];

  *count = ring_buffer_pending(b);

  return 0;
}
