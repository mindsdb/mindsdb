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

/* Include Python.h so we can set Python's error indicator. */
#define PY_SSIZE_T_CLEAN
#include "Python.h"

#include <stdlib.h>
#include <string.h>

#include "buffer.h"

#define INITIAL_BUFFER_SIZE 256

struct buffer {
    char* buffer;
    int size;
    int position;
};

/* Set Python's error indicator to MemoryError.
 * Called after allocation failures. */
static void set_memory_error(void) {
    PyErr_NoMemory();
}

/* Allocate and return a new buffer.
 * Return NULL and sets MemoryError on allocation failure. */
buffer_t pymongo_buffer_new(void) {
    buffer_t buffer;
    buffer = (buffer_t)malloc(sizeof(struct buffer));
    if (buffer == NULL) {
        set_memory_error();
        return NULL;
    }

    buffer->size = INITIAL_BUFFER_SIZE;
    buffer->position = 0;
    buffer->buffer = (char*)malloc(sizeof(char) * INITIAL_BUFFER_SIZE);
    if (buffer->buffer == NULL) {
        free(buffer);
        set_memory_error();
        return NULL;
    }

    return buffer;
}

/* Free the memory allocated for `buffer`.
 * Return non-zero on failure. */
int pymongo_buffer_free(buffer_t buffer) {
    if (buffer == NULL) {
        return 1;
    }
    /* Buffer will be NULL when buffer_grow fails. */
    if (buffer->buffer != NULL) {
        free(buffer->buffer);
    }
    free(buffer);
    return 0;
}

/* Grow `buffer` to at least `min_length`.
 * Return non-zero and sets MemoryError on allocation failure. */
static int buffer_grow(buffer_t buffer, int min_length) {
    int old_size = 0;
    int size = buffer->size;
    char* old_buffer = buffer->buffer;
    if (size >= min_length) {
        return 0;
    }
    while (size < min_length) {
        old_size = size;
        size *= 2;
        if (size <= old_size) {
           /* Size did not increase. Could be an overflow
            * or size < 1. Just go with min_length. */
           size = min_length;
        }
    }
    buffer->buffer = (char*)realloc(buffer->buffer, sizeof(char) * size);
    if (buffer->buffer == NULL) {
        free(old_buffer);
        set_memory_error();
        return 1;
    }
    buffer->size = size;
    return 0;
}

/* Assure that `buffer` has at least `size` free bytes (and grow if needed).
 * Return non-zero and sets MemoryError on allocation failure.
 * Return non-zero and sets ValueError if `size` would exceed 2GiB. */
static int buffer_assure_space(buffer_t buffer, int size) {
    int new_size = buffer->position + size;
    /* Check for overflow. */
    if (new_size < buffer->position) {
        PyErr_SetString(PyExc_ValueError,
                        "Document would overflow BSON size limit");
        return 1;
    }

    if (new_size <= buffer->size) {
        return 0;
    }
    return buffer_grow(buffer, new_size);
}

/* Save `size` bytes from the current position in `buffer` (and grow if needed).
 * Return offset for writing, or -1 on failure.
 * Sets MemoryError or ValueError on failure. */
buffer_position pymongo_buffer_save_space(buffer_t buffer, int size) {
    int position = buffer->position;
    if (buffer_assure_space(buffer, size) != 0) {
        return -1;
    }
    buffer->position += size;
    return position;
}

/* Write `size` bytes from `data` to `buffer` (and grow if needed).
 * Return non-zero on failure.
 * Sets MemoryError or ValueError on failure. */
int pymongo_buffer_write(buffer_t buffer, const char* data, int size) {
    if (buffer_assure_space(buffer, size) != 0) {
        return 1;
    }

    memcpy(buffer->buffer + buffer->position, data, size);
    buffer->position += size;
    return 0;
}

int pymongo_buffer_get_position(buffer_t buffer) {
    return buffer->position;
}

char* pymongo_buffer_get_buffer(buffer_t buffer) {
    return buffer->buffer;
}

void pymongo_buffer_update_position(buffer_t buffer, buffer_position new_position) {
    buffer->position = new_position;
}
