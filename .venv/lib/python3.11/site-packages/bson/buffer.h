/*
 * Copyright 2009-2015 MongoDB, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#ifndef BUFFER_H
#define BUFFER_H

/* Note: if any of these functions return a failure condition then the buffer
 * has already been freed. */

/* A buffer */
typedef struct buffer* buffer_t;
/* A position in the buffer */
typedef int buffer_position;

/* Allocate and return a new buffer.
 * Return NULL on allocation failure. */
buffer_t pymongo_buffer_new(void);

/* Free the memory allocated for `buffer`.
 * Return non-zero on failure. */
int pymongo_buffer_free(buffer_t buffer);

/* Save `size` bytes from the current position in `buffer` (and grow if needed).
 * Return offset for writing, or -1 on allocation failure. */
buffer_position pymongo_buffer_save_space(buffer_t buffer, int size);

/* Write `size` bytes from `data` to `buffer` (and grow if needed).
 * Return non-zero on allocation failure. */
int pymongo_buffer_write(buffer_t buffer, const char* data, int size);

/* Getters for the internals of a buffer_t.
 * Should try to avoid using these as much as possible
 * since they break the abstraction. */
buffer_position pymongo_buffer_get_position(buffer_t buffer);
char* pymongo_buffer_get_buffer(buffer_t buffer);
void pymongo_buffer_update_position(buffer_t buffer, buffer_position new_position);

#endif
