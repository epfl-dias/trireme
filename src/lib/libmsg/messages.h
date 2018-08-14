#if !defined(MESSAGES_H_)
#define MESSAGES_H_

#include "ring_buffer.h"
typedef uint32_t msg_errors_t;

struct box_array;

msg_errors_t msg_alloc(struct box_array ** boxes, const size_t max_clients, const size_t max_servers);
msg_errors_t msg_free(struct box_array ** boxes);

#define MSG_MAX_QUEUES	     2 /* We can go up to 8 without touching any other flags */
#define MSG_IN		0x0000
#define MSG_OUT		0x0001

#define MSG_BLOCK	0x0010
#define MSG_FLUSH	0x0100

msg_errors_t msg_send(struct box_array * boxes, const size_t client, const size_t server, uint32_t flags, const size_t count, const uint64_t * data);
msg_errors_t msg_receive(struct box_array * boxes, const size_t client, const size_t server, uint32_t flags, size_t * count, uint64_t * data);
msg_errors_t msg_pending(struct box_array * boxes, const size_t client, const size_t server, uint32_t flags, size_t * count);

msg_errors_t msg_flush(struct box_array * boxes, const size_t client, const size_t server, uint32_t flags);
#endif
