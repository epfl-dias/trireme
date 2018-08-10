#if !defined(RING_BUFFER_H_)
#define RING_BUFFER_H_

#if ((RING_BUFFER_SIZE % CACHELINE) != 0)
#error RING_BUFFER_SIZE must be aligned to CACHELINE
#endif

struct ring_buffer; /* Forward declaration */

struct box {
  struct ring_buffer * in;
  struct ring_buffer * out;
} __attribute__ ((aligned (CACHELINE)));

void ring_buffer_alloc(struct ring_buffer** buffer);
void ring_buffer_free(struct ring_buffer** buffer);

void ring_buffer_write(struct ring_buffer* buffer, uint64_t data);
void ring_buffer_write_all(struct ring_buffer* buffer, int32_t write_count, const uint64_t* data, int32_t force_flush);
void ring_buffer_flush(struct ring_buffer* buffer);

int32_t ring_buffer_read_all(struct ring_buffer* buffer, int32_t max_read_count, uint64_t* data, int32_t blocking);

int32_t ring_buffer_peek(struct ring_buffer* buffer, int32_t max_read_count, uint64_t* data);
void ring_buffer_seek(struct ring_buffer *buffer, int32_t count);

#endif
