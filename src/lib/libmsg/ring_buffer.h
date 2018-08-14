#if !defined(RING_BUFFER_H_)
#define RING_BUFFER_H_

#if !defined(MESSAGES_H_)
#error ring_buffer.h is a private header of libmsg
#endif /* !defined(MESSAGES_H_) */

#if ((RING_BUFFER_SIZE % CACHELINE) != 0)
#error RING_BUFFER_SIZE must be aligned to CACHELINE
#endif

struct ring_buffer; /* Forward declaration */

/* Allocate a ring buffer of size RING_BUFFER_SIZE.
 * buffer: OUT parameter. */
void ring_buffer_alloc(struct ring_buffer** buffer);

/* Free the ring buffer pointed to by buffer.
 * buffer: OUT parameter, on success it is set to NULL. */
void ring_buffer_free(struct ring_buffer** buffer);

/* Write the message data into the buffer. */
void ring_buffer_write(struct ring_buffer* buffer, uint64_t data);

/* Write 'count' messages stored in the 'data' array into the buffer. If
 * 'force_flush' is non-zero, a flush will be executed immediately. */
void ring_buffer_write_all(struct ring_buffer* buffer, int32_t write_count,
	const uint64_t* data, int32_t force_flush);

/* Flush the ring buffer, making sure the data is visible to the recipient. */
void ring_buffer_flush(struct ring_buffer* buffer);

/* Read up to 'max_read_count' messages in the 'data' array. If 'blocking' is
 * set to 0, then the function will always return immediately, only providing
 * the data currently available, instead of waiting to fill max_read_count
 * values.*/
int32_t ring_buffer_read_all(struct ring_buffer* buffer,
	int32_t max_read_count, uint64_t* data, int32_t blocking);

/* Return the number of messages currently available in the buffer. */
int32_t ring_buffer_pending(struct ring_buffer* buffer);

/* Seek the current read position to the current position plus 'count'. */
void ring_buffer_seek(struct ring_buffer *buffer, int32_t count);

#endif
