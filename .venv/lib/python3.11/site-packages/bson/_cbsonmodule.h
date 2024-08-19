/*
 * Copyright 2009-present MongoDB, Inc.
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

#include "bson-endian.h"

#ifndef _CBSONMODULE_H
#define _CBSONMODULE_H

#if defined(WIN32) || defined(_MSC_VER)
/*
 * This macro is basically an implementation of asprintf for win32
 * We print to the provided buffer to get the string value as an int.
 * USE LL2STR. This is kept only to test LL2STR.
 */
#if defined(_MSC_VER) && (_MSC_VER >= 1400)
#define INT2STRING(buffer, i)                                       \
    _snprintf_s((buffer),                                           \
                 _scprintf("%lld", (i)) + 1,                          \
                 _scprintf("%lld", (i)) + 1,                          \
                 "%lld",                                              \
                 (i))
#define STRCAT(dest, n, src) strcat_s((dest), (n), (src))
#else
#define INT2STRING(buffer, i)                                       \
    _snprintf((buffer),                                             \
               _scprintf("%lld", (i)) + 1,                            \
               "%lld",                                                \
              (i))
#define STRCAT(dest, n, src) strcat((dest), (src))
#endif
#else
#define INT2STRING(buffer, i) snprintf((buffer), sizeof((buffer)), "%lld", (i))
#define STRCAT(dest, n, src) strcat((dest), (src))
#endif

/* Just enough space in char array to hold LLONG_MIN and null terminator */
#define BUF_SIZE 21
/* Converts integer to its string representation in decimal notation. */
extern int cbson_long_long_to_str(long long int num, char* str, size_t size);
#define LL2STR(buffer, i) cbson_long_long_to_str((i), (buffer), sizeof(buffer))

typedef struct type_registry_t {
    PyObject* encoder_map;
    PyObject* decoder_map;
    PyObject* fallback_encoder;
    PyObject* registry_obj;
    unsigned char is_encoder_empty;
    unsigned char is_decoder_empty;
    unsigned char has_fallback_encoder;
} type_registry_t;

typedef struct codec_options_t {
    PyObject* document_class;
    unsigned char tz_aware;
    unsigned char uuid_rep;
    char* unicode_decode_error_handler;
    PyObject* tzinfo;
    type_registry_t type_registry;
    unsigned char datetime_conversion;
    PyObject* options_obj;
    unsigned char is_raw_bson;
} codec_options_t;

/* C API functions */
#define _cbson_buffer_write_bytes_INDEX 0
#define _cbson_buffer_write_bytes_RETURN int
#define _cbson_buffer_write_bytes_PROTO (buffer_t buffer, const char* data, int size)

#define _cbson_write_dict_INDEX 1
#define _cbson_write_dict_RETURN int
#define _cbson_write_dict_PROTO (PyObject* self, buffer_t buffer, PyObject* dict, unsigned char check_keys, const codec_options_t* options, unsigned char top_level)

#define _cbson_write_pair_INDEX 2
#define _cbson_write_pair_RETURN int
#define _cbson_write_pair_PROTO (PyObject* self, buffer_t buffer, const char* name, int name_length, PyObject* value, unsigned char check_keys, const codec_options_t* options, unsigned char allow_id)

#define _cbson_decode_and_write_pair_INDEX 3
#define _cbson_decode_and_write_pair_RETURN int
#define _cbson_decode_and_write_pair_PROTO (PyObject* self, buffer_t buffer, PyObject* key, PyObject* value, unsigned char check_keys, const codec_options_t* options, unsigned char top_level)

#define _cbson_convert_codec_options_INDEX 4
#define _cbson_convert_codec_options_RETURN int
#define _cbson_convert_codec_options_PROTO (PyObject* self, PyObject* options_obj, codec_options_t* options)

#define _cbson_destroy_codec_options_INDEX 5
#define _cbson_destroy_codec_options_RETURN void
#define _cbson_destroy_codec_options_PROTO (codec_options_t* options)

#define _cbson_buffer_write_double_INDEX 6
#define _cbson_buffer_write_double_RETURN int
#define _cbson_buffer_write_double_PROTO (buffer_t buffer, double data)

#define _cbson_buffer_write_int32_INDEX 7
#define _cbson_buffer_write_int32_RETURN int
#define _cbson_buffer_write_int32_PROTO (buffer_t buffer, int32_t data)

#define _cbson_buffer_write_int64_INDEX 8
#define _cbson_buffer_write_int64_RETURN int
#define _cbson_buffer_write_int64_PROTO (buffer_t buffer, int64_t data)

#define _cbson_buffer_write_int32_at_position_INDEX 9
#define _cbson_buffer_write_int32_at_position_RETURN void
#define _cbson_buffer_write_int32_at_position_PROTO (buffer_t buffer, int position, int32_t data)

#define _cbson_downcast_and_check_INDEX 10
#define _cbson_downcast_and_check_RETURN int
#define _cbson_downcast_and_check_PROTO (Py_ssize_t size, uint8_t extra)

/* Total number of C API pointers */
#define _cbson_API_POINTER_COUNT 11

#ifdef _CBSON_MODULE
/* This section is used when compiling _cbsonmodule */

static _cbson_buffer_write_bytes_RETURN buffer_write_bytes _cbson_buffer_write_bytes_PROTO;

static _cbson_write_dict_RETURN write_dict _cbson_write_dict_PROTO;

static _cbson_write_pair_RETURN write_pair _cbson_write_pair_PROTO;

static _cbson_decode_and_write_pair_RETURN decode_and_write_pair _cbson_decode_and_write_pair_PROTO;

static _cbson_convert_codec_options_RETURN convert_codec_options _cbson_convert_codec_options_PROTO;

static _cbson_destroy_codec_options_RETURN destroy_codec_options _cbson_destroy_codec_options_PROTO;

static _cbson_buffer_write_double_RETURN buffer_write_double _cbson_buffer_write_double_PROTO;

static _cbson_buffer_write_int32_RETURN buffer_write_int32 _cbson_buffer_write_int32_PROTO;

static _cbson_buffer_write_int64_RETURN buffer_write_int64 _cbson_buffer_write_int64_PROTO;

static _cbson_buffer_write_int32_at_position_RETURN buffer_write_int32_at_position _cbson_buffer_write_int32_at_position_PROTO;

static _cbson_downcast_and_check_RETURN _downcast_and_check _cbson_downcast_and_check_PROTO;

#else
/* This section is used in modules that use _cbsonmodule's API */

static void **_cbson_API;

#define buffer_write_bytes (*(_cbson_buffer_write_bytes_RETURN (*)_cbson_buffer_write_bytes_PROTO) _cbson_API[_cbson_buffer_write_bytes_INDEX])

#define write_dict (*(_cbson_write_dict_RETURN (*)_cbson_write_dict_PROTO) _cbson_API[_cbson_write_dict_INDEX])

#define write_pair (*(_cbson_write_pair_RETURN (*)_cbson_write_pair_PROTO) _cbson_API[_cbson_write_pair_INDEX])

#define decode_and_write_pair (*(_cbson_decode_and_write_pair_RETURN (*)_cbson_decode_and_write_pair_PROTO) _cbson_API[_cbson_decode_and_write_pair_INDEX])

#define convert_codec_options (*(_cbson_convert_codec_options_RETURN (*)_cbson_convert_codec_options_PROTO) _cbson_API[_cbson_convert_codec_options_INDEX])

#define destroy_codec_options (*(_cbson_destroy_codec_options_RETURN (*)_cbson_destroy_codec_options_PROTO) _cbson_API[_cbson_destroy_codec_options_INDEX])

#define buffer_write_double (*(_cbson_buffer_write_double_RETURN (*)_cbson_buffer_write_double_PROTO) _cbson_API[_cbson_buffer_write_double_INDEX])

#define buffer_write_int32 (*(_cbson_buffer_write_int32_RETURN (*)_cbson_buffer_write_int32_PROTO) _cbson_API[_cbson_buffer_write_int32_INDEX])

#define buffer_write_int64 (*(_cbson_buffer_write_int64_RETURN (*)_cbson_buffer_write_int64_PROTO) _cbson_API[_cbson_buffer_write_int64_INDEX])

#define buffer_write_int32_at_position (*(_cbson_buffer_write_int32_at_position_RETURN (*)_cbson_buffer_write_int32_at_position_PROTO) _cbson_API[_cbson_buffer_write_int32_at_position_INDEX])

#define _downcast_and_check (*(_cbson_downcast_and_check_RETURN (*)_cbson_downcast_and_check_PROTO) _cbson_API[_cbson_downcast_and_check_INDEX])

#define _cbson_IMPORT _cbson_API = (void **)PyCapsule_Import("_cbson._C_API", 0)

#endif

#endif // _CBSONMODULE_H
