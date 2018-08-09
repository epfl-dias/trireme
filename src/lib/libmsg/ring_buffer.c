#include <assert.h>
#include <stdlib.h>
#include <stdint.h>

#include <emmintrin.h> /* For _mm_pause */

#include "const.h"
#include "ring_buffer.h"

/* Public functions */
void
ring_buffer_write(struct ring_buffer* buffer, uint64_t data)
{
  // wait till there is space in buffer
  assert (buffer->tmp_wr_index < buffer->rd_index + RING_BUFFER_SIZE);

  buffer->data[buffer->tmp_wr_index & (RING_BUFFER_SIZE - 1)] = data;
  buffer->tmp_wr_index++;
  if (buffer->tmp_wr_index >= buffer->wr_index + BUFFER_FLUSH_COUNT) {
    ring_buffer_flush(buffer);
  }
}

void
ring_buffer_write_all(struct ring_buffer* buffer, int32_t write_count, const uint64_t* data, int32_t force_flush) 
{
  assert(write_count <= RING_BUFFER_SIZE);

  /* while (buffer->tmp_wr_index + write_count - 1 >= buffer->rd_index + RING_BUFFER_SIZE) {
    _mm_pause();
  }
  */
  assert (buffer->tmp_wr_index + write_count - 1 < buffer->rd_index + RING_BUFFER_SIZE); 

  for (int32_t i = 0; i < write_count; i++) {
    buffer->data[(buffer->tmp_wr_index + i) & (RING_BUFFER_SIZE - 1)] = data[i];
  }
  buffer->tmp_wr_index += write_count;
  if (force_flush || (buffer->tmp_wr_index >= buffer->wr_index + BUFFER_FLUSH_COUNT)) {
    ring_buffer_flush(buffer);
  }
}

void
ring_buffer_flush(struct ring_buffer* buffer)
{

  if (buffer->wr_index == buffer->tmp_wr_index)
    return;

  // for safety do memory barrier to make sure data is written before index
  __sync_synchronize();

  buffer->wr_index = buffer->tmp_wr_index;
}

int32_t
ring_buffer_read_all(struct ring_buffer* buffer, int32_t max_read_count, uint64_t* data, int32_t blocking)
{
  int32_t count = buffer->wr_index - buffer->rd_index;
  if (count == 0 && !blocking)
    return 0;

  while (!(count = buffer->wr_index - buffer->rd_index)) {
    _mm_pause();
  }

  if (max_read_count < count) count = max_read_count;

  for (int32_t i = 0; i < count; i++) {
    data[i] = buffer->data[(buffer->rd_index + i) & (RING_BUFFER_SIZE - 1)];
  }
  buffer->rd_index += count;
  return count;
}


int32_t
ring_buffer_peek(struct ring_buffer* buffer, int32_t max_read_count, uint64_t* data)
{
  int32_t count = buffer->wr_index - buffer->rd_index;

  if (!count)
    return 0;

  if (max_read_count < count) count = max_read_count;

  for (int32_t i = 0; i < count; i++) {
    data[i] = buffer->data[(buffer->rd_index + i) & (RING_BUFFER_SIZE - 1)];
  }

  return count;
}

void
ring_buffer_seek(struct ring_buffer *buffer, int32_t count)
{
  buffer->rd_index += count;
}
