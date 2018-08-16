#include <assert.h>
#include <stdlib.h>

#include "headers.h"
#include "onewaybuffer.h"

void buffer_write(struct onewaybuffer* buffer, uint64_t data)
{
  // wait till there is space in buffer
  assert (buffer->tmp_wr_index < buffer->rd_index + ONEWAY_BUFFER_SIZE);

  buffer->data[buffer->tmp_wr_index & (ONEWAY_BUFFER_SIZE - 1)] = data;
  buffer->tmp_wr_index++;
  if (buffer->tmp_wr_index >= buffer->wr_index + BUFFER_FLUSH_COUNT) {
    buffer_flush(buffer);
  }
}

void buffer_write_all(struct onewaybuffer* buffer, int write_count, const uint64_t* data, int force_flush) 
{
  assert(write_count <= ONEWAY_BUFFER_SIZE);
 
  /* while (buffer->tmp_wr_index + write_count - 1 >= buffer->rd_index + ONEWAY_BUFFER_SIZE) {
    _mm_pause();
  }
  */
  assert (buffer->tmp_wr_index + write_count - 1 < buffer->rd_index + ONEWAY_BUFFER_SIZE); 

  for (int i = 0; i < write_count; i++) {
    buffer->data[(buffer->tmp_wr_index + i) & (ONEWAY_BUFFER_SIZE - 1)] = data[i];
  }
  buffer->tmp_wr_index += write_count;
  if (force_flush || (buffer->tmp_wr_index >= buffer->wr_index + BUFFER_FLUSH_COUNT)) {
    buffer_flush(buffer);
  }
}

void buffer_flush(struct onewaybuffer* buffer)
{

  if (buffer->wr_index == buffer->tmp_wr_index)
    return;

  // for safety do memory barrier to make sure data is written before index
  __sync_synchronize(); 

  buffer->wr_index = buffer->tmp_wr_index;
}

int buffer_read_all(struct onewaybuffer* buffer, int max_read_count, uint64_t* data, int blocking)
{
  int count = buffer->wr_index - buffer->rd_index;
  if (count == 0 && !blocking)
    return 0;

  while (!(count = buffer->wr_index - buffer->rd_index)) {
    _mm_pause();
  } 
      
  if (max_read_count < count) count = max_read_count;

  for (int i = 0; i < count; i++) {
    data[i] = buffer->data[(buffer->rd_index + i) & (ONEWAY_BUFFER_SIZE - 1)];
  }
  buffer->rd_index += count;
  return count;
}


int buffer_peek(struct onewaybuffer* buffer, int max_read_count, uint64_t* data)
{
  int count = buffer->wr_index - buffer->rd_index;
  
  if (!count)
    return 0;

  if (max_read_count < count) count = max_read_count;

  for (int i = 0; i < count; i++) {
    data[i] = buffer->data[(buffer->rd_index + i) & (ONEWAY_BUFFER_SIZE - 1)];
  }

  return count;
}

void buffer_seek(struct onewaybuffer *buffer, int count)
{
  buffer->rd_index += count;
}
