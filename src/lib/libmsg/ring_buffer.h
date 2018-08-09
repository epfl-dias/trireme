#ifndef RING_BUFFER_H_
#define RING_BUFFER_H_

#if !defined(LIBMSG)
//#error "Including private header of libmsg"
#endif

struct ring_buffer; /* Forward declaration */

void ring_buffer_write(struct ring_buffer* buffer, uint64_t data);
void ring_buffer_write_all(struct ring_buffer* buffer, int write_count, const uint64_t* data, int force_flush);
void ring_buffer_flush(struct ring_buffer* buffer);
int ring_buffer_read_all(struct ring_buffer* buffer, int max_read_count, uint64_t* data, int blocking);
int ring_buffer_peek(struct ring_buffer* buffer, int max_read_count, uint64_t* data);
void ring_buffer_seek(struct ring_buffer *buffer, int count);

/* Private data types */
/* Sadly, not yet so private. */

struct ring_buffer {
  volatile uint64_t data[RING_BUFFER_SIZE];
  volatile unsigned long rd_index;
  volatile char padding0[CACHELINE - sizeof(long)];
  volatile unsigned long wr_index;
  volatile char padding1[CACHELINE - sizeof(long)];
  volatile unsigned long tmp_wr_index;
  volatile char padding2[CACHELINE - sizeof(long)];
} __attribute__ ((aligned (CACHELINE)));

struct box {
  struct ring_buffer in;
  struct ring_buffer out;
}  __attribute__ ((aligned (CACHELINE)));

#endif
