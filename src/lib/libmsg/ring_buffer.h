#ifndef RING_BUFFER_H_
#define RING_BUFFER_H_

#if !defined(LIBMSG)
//#error "Including private header of libmsg"
#endif

struct ring_buffer; /* Forward declaration */

void ring_buffer_write(struct ring_buffer* buffer, uint64_t data);
void ring_buffer_write_all(struct ring_buffer* buffer, int32_t write_count, const uint64_t* data, int32_t force_flush);
void ring_buffer_flush(struct ring_buffer* buffer);
int32_t ring_buffer_read_all(struct ring_buffer* buffer, int32_t max_read_count, uint64_t* data, int32_t blocking);
int32_t ring_buffer_peek(struct ring_buffer* buffer, int32_t max_read_count, uint64_t* data);
void ring_buffer_seek(struct ring_buffer *buffer, int32_t count);

/* Private data types */
/* Sadly, not yet so private. */

struct ring_buffer {
  volatile uint64_t data[RING_BUFFER_SIZE];
  volatile uint32_t rd_index;
  volatile uint8_t padding0[CACHELINE - sizeof(uint32_t)];
  volatile uint32_t wr_index;
  volatile uint8_t padding1[CACHELINE - sizeof(uint32_t)];
  volatile uint32_t tmp_wr_index;
  volatile uint8_t padding2[CACHELINE - sizeof(uint32_t)];
} __attribute__ ((aligned (CACHELINE)));

struct box {
  struct ring_buffer in;
  struct ring_buffer out;
} __attribute__ ((aligned (CACHELINE)));

#endif
