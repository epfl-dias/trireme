#ifndef __ONEWAYBUFFER_H_
#define __ONEWAYBUFFER_H_

void buffer_write(struct onewaybuffer* buffer, uint64_t data);
void buffer_write_all(struct onewaybuffer* buffer, int write_count, const uint64_t* data, int force_flush);
void buffer_flush(struct onewaybuffer* buffer);
int buffer_read_all(struct onewaybuffer* buffer, int max_read_count, uint64_t* data, int blocking);
int buffer_peek(struct onewaybuffer* buffer, int max_read_count, uint64_t* data);
void buffer_seek(struct onewaybuffer *buffer, int count);

#endif
