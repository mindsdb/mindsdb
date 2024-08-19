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

/*
 * This file contains C implementations of some of the functions
 * needed by the bson module. If possible, these implementations
 * should be used to speed up BSON encoding and decoding.
 */

#define PY_SSIZE_T_CLEAN
#include "Python.h"
#include "datetime.h"

#include "buffer.h"
#include "time64.h"

#define _CBSON_MODULE
#include "_cbsonmodule.h"

/* New module state and initialization code.
 * See the module-initialization-and-state
 * section in the following doc:
 * http://docs.python.org/release/3.1.3/howto/cporting.html
 * which references the following pep:
 * http://www.python.org/dev/peps/pep-3121/
 * */
struct module_state {
    PyObject* Binary;
    PyObject* Code;
    PyObject* ObjectId;
    PyObject* DBRef;
    PyObject* Regex;
    PyObject* UUID;
    PyObject* Timestamp;
    PyObject* MinKey;
    PyObject* MaxKey;
    PyObject* UTC;
    PyTypeObject* REType;
    PyObject* BSONInt64;
    PyObject* Decimal128;
    PyObject* Mapping;
    PyObject* DatetimeMS;
    PyObject* _min_datetime_ms;
    PyObject* _max_datetime_ms;
    PyObject* _type_marker_str;
    PyObject* _flags_str;
    PyObject* _pattern_str;
    PyObject* _encoder_map_str;
    PyObject* _decoder_map_str;
    PyObject* _fallback_encoder_str;
    PyObject* _raw_str;
    PyObject* _subtype_str;
    PyObject* _binary_str;
    PyObject* _scope_str;
    PyObject* _inc_str;
    PyObject* _time_str;
    PyObject* _bid_str;
    PyObject* _replace_str;
    PyObject* _astimezone_str;
    PyObject* _id_str;
    PyObject* _dollar_ref_str;
    PyObject* _dollar_id_str;
    PyObject* _dollar_db_str;
    PyObject* _tzinfo_str;
    PyObject* _as_doc_str;
    PyObject* _utcoffset_str;
    PyObject* _from_uuid_str;
    PyObject* _as_uuid_str;
    PyObject* _from_bid_str;
};

#define GETSTATE(m) ((struct module_state*)PyModule_GetState(m))

/* Maximum number of regex flags */
#define FLAGS_SIZE 7

/* Default UUID representation type code. */
#define PYTHON_LEGACY 3

/* Other UUID representations. */
#define STANDARD 4
#define JAVA_LEGACY   5
#define CSHARP_LEGACY 6
#define UNSPECIFIED 0

#define BSON_MAX_SIZE 2147483647
/* The smallest possible BSON document, i.e. "{}" */
#define BSON_MIN_SIZE 5

/* Datetime codec options */
#define DATETIME 1
#define DATETIME_CLAMP 2
#define DATETIME_MS 3
#define DATETIME_AUTO 4

/* Converts integer to its string representation in decimal notation. */
extern int cbson_long_long_to_str(long long num, char* str, size_t size) {
    // Buffer should fit 64-bit signed integer
    if (size < 21) {
        PyErr_Format(
            PyExc_RuntimeError,
            "Buffer too small to hold long long: %d < 21", size);
        return -1;
    }
    int index = 0;
    int sign = 1;
    // Convert to unsigned to handle -LLONG_MIN overflow
    unsigned long long absNum;
    // Handle the case of 0
    if (num == 0) {
        str[index++] = '0';
        str[index] = '\0';
        return 0;
    }
    // Handle negative numbers
    if (num < 0) {
        sign = -1;
        absNum = 0ULL - (unsigned long long)num;
    } else {
        absNum = (unsigned long long)num;
    }
    // Convert the number to string
    unsigned long long digit;
    while (absNum > 0) {
        digit = absNum % 10ULL;
        str[index++] = (char)digit + '0';  // Convert digit to character
        absNum /= 10;
    }
    // Add minus sign if negative
    if (sign == -1) {
        str[index++] = '-';
    }
    str[index] = '\0';  // Null terminator
    // Reverse the string
    int start = 0;
    int end = index - 1;
    while (start < end) {
        char temp = str[start];
        str[start++] = str[end];
        str[end--] = temp;
    }
    return 0;
}

static PyObject* _test_long_long_to_str(PyObject* self, PyObject* args) {
    // Test extreme values
    Py_ssize_t maxNum = PY_SSIZE_T_MAX;
    Py_ssize_t minNum = PY_SSIZE_T_MIN;
    Py_ssize_t num;
    char str_1[BUF_SIZE];
    char str_2[BUF_SIZE];
    int res = LL2STR(str_1, (long long)minNum);
    if (res == -1) {
        return NULL;
    }
    INT2STRING(str_2, (long long)minNum);
    if (strcmp(str_1, str_2) != 0) {
        PyErr_Format(
            PyExc_RuntimeError,
            "LL2STR != INT2STRING: %s != %s", str_1, str_2);
        return NULL;
    }
    LL2STR(str_1, (long long)maxNum);
    INT2STRING(str_2, (long long)maxNum);
    if (strcmp(str_1, str_2) != 0) {
        PyErr_Format(
            PyExc_RuntimeError,
            "LL2STR != INT2STRING: %s != %s", str_1, str_2);
        return NULL;
    }

    // Test common values
    for (num = 0; num < 10000; num++) {
        char str_1[BUF_SIZE];
        char str_2[BUF_SIZE];
        LL2STR(str_1, (long long)num);
        INT2STRING(str_2, (long long)num);
        if (strcmp(str_1, str_2) != 0) {
            PyErr_Format(
                PyExc_RuntimeError,
                "LL2STR != INT2STRING: %s != %s", str_1, str_2);
            return NULL;
        }
    }

    return args;
}

/* Get an error class from the bson.errors module.
 *
 * Returns a new ref */
static PyObject* _error(char* name) {
    PyObject* error;
    PyObject* errors = PyImport_ImportModule("bson.errors");
    if (!errors) {
        return NULL;
    }
    error = PyObject_GetAttrString(errors, name);
    Py_DECREF(errors);
    return error;
}

/* Safely downcast from Py_ssize_t to int, setting an
 * exception and returning -1 on error. */
static int
_downcast_and_check(Py_ssize_t size, uint8_t extra) {
    if (size > BSON_MAX_SIZE || ((BSON_MAX_SIZE - extra) < size)) {
        PyObject* InvalidStringData = _error("InvalidStringData");
        if (InvalidStringData) {
            PyErr_SetString(InvalidStringData,
                            "String length must be <= 2147483647");
            Py_DECREF(InvalidStringData);
        }
        return -1;
    }
    return (int)size + extra;
}

static PyObject* elements_to_dict(PyObject* self, const char* string,
                                  unsigned max,
                                  const codec_options_t* options);

static int _write_element_to_buffer(PyObject* self, buffer_t buffer,
                                    int type_byte, PyObject* value,
                                    unsigned char check_keys,
                                    const codec_options_t* options,
                                    unsigned char in_custom_call,
                                    unsigned char in_fallback_call);

/* Write a RawBSONDocument to the buffer.
 * Returns the number of bytes written or 0 on failure.
 */
static int write_raw_doc(buffer_t buffer, PyObject* raw, PyObject* _raw);

/* Date stuff */
static PyObject* datetime_from_millis(long long millis) {
    /* To encode a datetime instance like datetime(9999, 12, 31, 23, 59, 59, 999999)
     * we follow these steps:
     * 1. Calculate a timestamp in seconds:       253402300799
     * 2. Multiply that by 1000:                  253402300799000
     * 3. Add in microseconds divided by 1000     253402300799999
     *
     * (Note: BSON doesn't support microsecond accuracy, hence the rounding.)
     *
     * To decode we could do:
     * 1. Get seconds: timestamp / 1000:          253402300799
     * 2. Get micros: (timestamp % 1000) * 1000:  999000
     * Resulting in datetime(9999, 12, 31, 23, 59, 59, 999000) -- the expected result
     *
     * Now what if the we encode (1, 1, 1, 1, 1, 1, 111111)?
     * 1. and 2. gives:                           -62135593139000
     * 3. Gives us:                               -62135593138889
     *
     * Now decode:
     * 1. Gives us:                               -62135593138
     * 2. Gives us:                               -889000
     * Resulting in datetime(1, 1, 1, 1, 1, 2, 15888216) -- an invalid result
     *
     * If instead to decode we do:
     * diff = ((millis % 1000) + 1000) % 1000:    111
     * seconds = (millis - diff) / 1000:          -62135593139
     * micros = diff * 1000                       111000
     * Resulting in datetime(1, 1, 1, 1, 1, 1, 111000) -- the expected result
     */
    PyObject* datetime;
    int diff = (int)(((millis % 1000) + 1000) % 1000);
    int microseconds = diff * 1000;
    Time64_T seconds = (millis - diff) / 1000;
    struct TM timeinfo;
    cbson_gmtime64_r(&seconds, &timeinfo);

    datetime = PyDateTime_FromDateAndTime(timeinfo.tm_year + 1900,
                                          timeinfo.tm_mon + 1,
                                          timeinfo.tm_mday,
                                          timeinfo.tm_hour,
                                          timeinfo.tm_min,
                                          timeinfo.tm_sec,
                                          microseconds);
    if(!datetime) {
        PyObject *etype, *evalue, *etrace;

        /*
        * Calling _error clears the error state, so fetch it first.
        */
        PyErr_Fetch(&etype, &evalue, &etrace);

        /* Only add addition error message on ValueError exceptions. */
        if (PyErr_GivenExceptionMatches(etype, PyExc_ValueError)) {
            if (evalue) {
                PyObject* err_msg = PyObject_Str(evalue);
                if (err_msg) {
                    PyObject* appendage = PyUnicode_FromString(" (Consider Using CodecOptions(datetime_conversion=DATETIME_AUTO) or MongoClient(datetime_conversion='DATETIME_AUTO')). See: https://pymongo.readthedocs.io/en/stable/examples/datetimes.html#handling-out-of-range-datetimes");
                    if (appendage) {
                        PyObject* msg = PyUnicode_Concat(err_msg, appendage);
                        if (msg) {
                            Py_DECREF(evalue);
                            evalue = msg;
                        }
                    }
                    Py_XDECREF(appendage);
                }
                Py_XDECREF(err_msg);
            }
            PyErr_NormalizeException(&etype, &evalue, &etrace);
        }
        /* Steals references to args. */
        PyErr_Restore(etype, evalue, etrace);
    }
    return datetime;
}

static long long millis_from_datetime(PyObject* datetime) {
    struct TM timeinfo;
    long long millis;

    timeinfo.tm_year = PyDateTime_GET_YEAR(datetime) - 1900;
    timeinfo.tm_mon = PyDateTime_GET_MONTH(datetime) - 1;
    timeinfo.tm_mday = PyDateTime_GET_DAY(datetime);
    timeinfo.tm_hour = PyDateTime_DATE_GET_HOUR(datetime);
    timeinfo.tm_min = PyDateTime_DATE_GET_MINUTE(datetime);
    timeinfo.tm_sec = PyDateTime_DATE_GET_SECOND(datetime);

    millis = cbson_timegm64(&timeinfo) * 1000;
    millis += PyDateTime_DATE_GET_MICROSECOND(datetime) / 1000;
    return millis;
}

/* Extended-range datetime, returns a DatetimeMS object with millis */
static PyObject* datetime_ms_from_millis(PyObject* self, long long millis){
    // Allocate a new DatetimeMS object.
    struct module_state *state = GETSTATE(self);
    if (!state) {
        return NULL;
    }

    PyObject* dt;
    PyObject* ll_millis;

    if (!(ll_millis = PyLong_FromLongLong(millis))){
        return NULL;
    }
    dt = PyObject_CallFunctionObjArgs(state->DatetimeMS, ll_millis, NULL);
    Py_DECREF(ll_millis);
    return dt;
}

/* Extended-range datetime, takes a DatetimeMS object and extracts the long long value. */
static int millis_from_datetime_ms(PyObject* dt, long long* out){
    PyObject* ll_millis;
    long long millis;

    if (!(ll_millis = PyNumber_Long(dt))){
        return 0;
    }
    millis = PyLong_AsLongLong(ll_millis);
    Py_DECREF(ll_millis);
    if (millis == -1 && PyErr_Occurred()) { /* Overflow */
        PyErr_SetString(PyExc_OverflowError,
                        "MongoDB datetimes can only handle up to 8-byte ints");
        return 0;
    }
    *out = millis;
    return 1;
}

/* Just make this compatible w/ the old API. */
int buffer_write_bytes(buffer_t buffer, const char* data, int size) {
    if (pymongo_buffer_write(buffer, data, size)) {
        return 0;
    }
    return 1;
}

int buffer_write_double(buffer_t buffer, double data) {
    double data_le = BSON_DOUBLE_TO_LE(data);
    return buffer_write_bytes(buffer, (const char*)&data_le, 8);
}

int buffer_write_int32(buffer_t buffer, int32_t data) {
    uint32_t data_le = BSON_UINT32_TO_LE(data);
    return buffer_write_bytes(buffer, (const char*)&data_le, 4);
}

int buffer_write_int64(buffer_t buffer, int64_t data) {
    uint64_t data_le = BSON_UINT64_TO_LE(data);
    return buffer_write_bytes(buffer, (const char*)&data_le, 8);
}

void buffer_write_int32_at_position(buffer_t buffer,
                                    int position,
                                    int32_t data) {
    uint32_t data_le = BSON_UINT32_TO_LE(data);
    memcpy(pymongo_buffer_get_buffer(buffer) + position, &data_le, 4);
}

static int write_unicode(buffer_t buffer, PyObject* py_string) {
    int size;
    const char* data;
    PyObject* encoded = PyUnicode_AsUTF8String(py_string);
    if (!encoded) {
        return 0;
    }
    data = PyBytes_AS_STRING(encoded);
    if (!data)
        goto unicodefail;

    if ((size = _downcast_and_check(PyBytes_GET_SIZE(encoded), 1)) == -1)
        goto unicodefail;

    if (!buffer_write_int32(buffer, (int32_t)size))
        goto unicodefail;

    if (!buffer_write_bytes(buffer, data, size))
        goto unicodefail;

    Py_DECREF(encoded);
    return 1;

unicodefail:
    Py_DECREF(encoded);
    return 0;
}

/* returns 0 on failure */
static int write_string(buffer_t buffer, PyObject* py_string) {
    int size;
    const char* data;
    if (PyUnicode_Check(py_string)){
        return write_unicode(buffer, py_string);
    }
    data = PyBytes_AsString(py_string);
    if (!data) {
        return 0;
    }

    if ((size = _downcast_and_check(PyBytes_Size(py_string), 1)) == -1)
        return 0;

    if (!buffer_write_int32(buffer, (int32_t)size)) {
        return 0;
    }
    if (!buffer_write_bytes(buffer, data, size)) {
        return 0;
    }
    return 1;
}

/* Load a Python object to cache.
 *
 * Returns non-zero on failure. */
static int _load_object(PyObject** object, char* module_name, char* object_name) {
    PyObject* module;

    module = PyImport_ImportModule(module_name);
    if (!module) {
        return 1;
    }

    *object = PyObject_GetAttrString(module, object_name);
    Py_DECREF(module);

    return (*object) ? 0 : 2;
}

/* Load all Python objects to cache.
 *
 * Returns non-zero on failure. */
static int _load_python_objects(PyObject* module) {
    PyObject* empty_string = NULL;
    PyObject* re_compile = NULL;
    PyObject* compiled = NULL;
    struct module_state *state = GETSTATE(module);
    if (!state) {
        return 1;
    }

    /* Cache commonly used attribute names to improve performance. */
    if (!((state->_type_marker_str = PyUnicode_FromString("_type_marker")) &&
        (state->_flags_str = PyUnicode_FromString("flags")) &&
        (state->_pattern_str = PyUnicode_FromString("pattern")) &&
        (state->_encoder_map_str = PyUnicode_FromString("_encoder_map")) &&
        (state->_decoder_map_str = PyUnicode_FromString("_decoder_map")) &&
        (state->_fallback_encoder_str = PyUnicode_FromString("_fallback_encoder")) &&
        (state->_raw_str = PyUnicode_FromString("raw")) &&
        (state->_subtype_str = PyUnicode_FromString("subtype")) &&
        (state->_binary_str = PyUnicode_FromString("binary")) &&
        (state->_scope_str = PyUnicode_FromString("scope")) &&
        (state->_inc_str = PyUnicode_FromString("inc")) &&
        (state->_time_str = PyUnicode_FromString("time")) &&
        (state->_bid_str = PyUnicode_FromString("bid")) &&
        (state->_replace_str = PyUnicode_FromString("replace")) &&
        (state->_astimezone_str = PyUnicode_FromString("astimezone")) &&
        (state->_id_str = PyUnicode_FromString("_id")) &&
        (state->_dollar_ref_str = PyUnicode_FromString("$ref")) &&
        (state->_dollar_id_str = PyUnicode_FromString("$id")) &&
        (state->_dollar_db_str = PyUnicode_FromString("$db")) &&
        (state->_tzinfo_str = PyUnicode_FromString("tzinfo")) &&
        (state->_as_doc_str = PyUnicode_FromString("as_doc")) &&
        (state->_utcoffset_str = PyUnicode_FromString("utcoffset")) &&
        (state->_from_uuid_str = PyUnicode_FromString("from_uuid")) &&
        (state->_as_uuid_str = PyUnicode_FromString("as_uuid")) &&
        (state->_from_bid_str = PyUnicode_FromString("from_bid")))) {
            return 1;
    }

    if (_load_object(&state->Binary, "bson.binary", "Binary") ||
        _load_object(&state->Code, "bson.code", "Code") ||
        _load_object(&state->ObjectId, "bson.objectid", "ObjectId") ||
        _load_object(&state->DBRef, "bson.dbref", "DBRef") ||
        _load_object(&state->Timestamp, "bson.timestamp", "Timestamp") ||
        _load_object(&state->MinKey, "bson.min_key", "MinKey") ||
        _load_object(&state->MaxKey, "bson.max_key", "MaxKey") ||
        _load_object(&state->UTC, "bson.tz_util", "utc") ||
        _load_object(&state->Regex, "bson.regex", "Regex") ||
        _load_object(&state->BSONInt64, "bson.int64", "Int64") ||
        _load_object(&state->Decimal128, "bson.decimal128", "Decimal128") ||
        _load_object(&state->UUID, "uuid", "UUID") ||
        _load_object(&state->Mapping, "collections.abc", "Mapping") ||
        _load_object(&state->DatetimeMS, "bson.datetime_ms", "DatetimeMS") ||
        _load_object(&state->_min_datetime_ms, "bson.datetime_ms", "_min_datetime_ms") ||
        _load_object(&state->_max_datetime_ms, "bson.datetime_ms", "_max_datetime_ms")) {
        return 1;
    }
    /* Reload our REType hack too. */
    empty_string = PyBytes_FromString("");
    if (empty_string == NULL) {
        state->REType = NULL;
        return 1;
    }

    if (_load_object(&re_compile, "re", "compile")) {
        state->REType = NULL;
        Py_DECREF(empty_string);
        return 1;
    }

    compiled = PyObject_CallFunction(re_compile, "O", empty_string);
    Py_DECREF(re_compile);
    if (compiled == NULL) {
        state->REType = NULL;
        Py_DECREF(empty_string);
        return 1;
    }
    Py_INCREF(Py_TYPE(compiled));
    state->REType = Py_TYPE(compiled);
    Py_DECREF(empty_string);
    Py_DECREF(compiled);
    return 0;
}

/*
 * Get the _type_marker from an Object.
 *
 * Return the type marker, 0 if there is no marker, or -1 on failure.
 */
static long _type_marker(PyObject* object, PyObject* _type_marker_str) {
    PyObject* type_marker = NULL;
    long type = 0;

    if (PyObject_HasAttr(object, _type_marker_str)) {
        type_marker = PyObject_GetAttr(object, _type_marker_str);
        if (type_marker == NULL) {
            return -1;
        }
    }

    /*
     * Python objects with broken __getattr__ implementations could return
     * arbitrary types for a call to PyObject_GetAttrString. For example
     * pymongo.database.Database returns a new Collection instance for
     * __getattr__ calls with names that don't match an existing attribute
     * or method. In some cases "value" could be a subtype of something
     * we know how to serialize. Make a best effort to encode these types.
     */
    if (type_marker && PyLong_CheckExact(type_marker)) {
        type = PyLong_AsLong(type_marker);
        Py_DECREF(type_marker);
    } else {
        Py_XDECREF(type_marker);
    }

    return type;
}

/* Fill out a type_registry_t* from a TypeRegistry object.
 *
 * Return 1 on success. options->document_class is a new reference.
 * Return 0 on failure.
 */
int cbson_convert_type_registry(PyObject* registry_obj, type_registry_t* registry, PyObject* _encoder_map_str, PyObject* _decoder_map_str, PyObject* _fallback_encoder_str) {
    registry->encoder_map = NULL;
    registry->decoder_map = NULL;
    registry->fallback_encoder = NULL;
    registry->registry_obj = NULL;

    registry->encoder_map = PyObject_GetAttr(registry_obj, _encoder_map_str);
    if (registry->encoder_map == NULL) {
        goto fail;
    }
    registry->is_encoder_empty = (PyDict_Size(registry->encoder_map) == 0);

    registry->decoder_map = PyObject_GetAttr(registry_obj, _decoder_map_str);
    if (registry->decoder_map == NULL) {
        goto fail;
    }
    registry->is_decoder_empty = (PyDict_Size(registry->decoder_map) == 0);

    registry->fallback_encoder = PyObject_GetAttr(registry_obj, _fallback_encoder_str);
    if (registry->fallback_encoder == NULL) {
        goto fail;
    }
    registry->has_fallback_encoder = (registry->fallback_encoder != Py_None);

    registry->registry_obj = registry_obj;
    Py_INCREF(registry->registry_obj);
    return 1;

fail:
    Py_XDECREF(registry->encoder_map);
    Py_XDECREF(registry->decoder_map);
    Py_XDECREF(registry->fallback_encoder);
    return 0;
}

/* Fill out a codec_options_t* from a CodecOptions object.
 *
 * Return 1 on success. options->document_class is a new reference.
 * Return 0 on failure.
 */
int convert_codec_options(PyObject* self, PyObject* options_obj, codec_options_t* options) {
    PyObject* type_registry_obj = NULL;
    struct module_state *state = GETSTATE(self);
    long type_marker;
    if (!state) {
        return 0;
    }

    options->unicode_decode_error_handler = NULL;

    if (!PyArg_ParseTuple(options_obj, "ObbzOOb",
                          &options->document_class,
                          &options->tz_aware,
                          &options->uuid_rep,
                          &options->unicode_decode_error_handler,
                          &options->tzinfo,
                          &type_registry_obj,
                          &options->datetime_conversion)) {
        return 0;
    }

    type_marker = _type_marker(options->document_class,
                               state->_type_marker_str);
    if (type_marker < 0) {
        return 0;
    }

    if (!cbson_convert_type_registry(type_registry_obj,
                               &options->type_registry, state->_encoder_map_str, state->_decoder_map_str, state->_fallback_encoder_str)) {
        return 0;
    }

    options->is_raw_bson = (101 == type_marker);
    options->options_obj = options_obj;

    Py_INCREF(options->options_obj);
    Py_INCREF(options->document_class);
    Py_INCREF(options->tzinfo);

    return 1;
}

void destroy_codec_options(codec_options_t* options) {
    Py_CLEAR(options->document_class);
    Py_CLEAR(options->tzinfo);
    Py_CLEAR(options->options_obj);
    Py_CLEAR(options->type_registry.registry_obj);
    Py_CLEAR(options->type_registry.encoder_map);
    Py_CLEAR(options->type_registry.decoder_map);
    Py_CLEAR(options->type_registry.fallback_encoder);
}

static int write_element_to_buffer(PyObject* self, buffer_t buffer,
                                   int type_byte, PyObject* value,
                                   unsigned char check_keys,
                                   const codec_options_t* options,
                                   unsigned char in_custom_call,
                                   unsigned char in_fallback_call) {
    int result = 0;
    if(Py_EnterRecursiveCall(" while encoding an object to BSON ")) {
        return 0;
    }
    result = _write_element_to_buffer(self, buffer, type_byte,
                                      value, check_keys, options,
                                      in_custom_call, in_fallback_call);
    Py_LeaveRecursiveCall();
    return result;
}

static void
_set_cannot_encode(PyObject* value) {
    if (PyLong_Check(value)) {
        if ((PyLong_AsLongLong(value) == -1) && PyErr_Occurred()) {
            return PyErr_SetString(PyExc_OverflowError,
                    "MongoDB can only handle up to 8-byte ints");
        }
    }

    PyObject* type = NULL;
    PyObject* InvalidDocument = _error("InvalidDocument");
    if (InvalidDocument == NULL) {
        goto error;
    }

    type = PyObject_Type(value);
    if (type == NULL) {
        goto error;
    }
    PyErr_Format(InvalidDocument, "cannot encode object: %R, of type: %R",
                 value, type);
error:
    Py_XDECREF(type);
    Py_XDECREF(InvalidDocument);
}

/*
 * Encode a builtin Python regular expression or our custom Regex class.
 *
 * Sets exception and returns 0 on failure.
 */
static int _write_regex_to_buffer(
    buffer_t buffer, int type_byte, PyObject* value, PyObject* _flags_str, PyObject* _pattern_str) {

    PyObject* py_flags;
    PyObject* py_pattern;
    PyObject* encoded_pattern;
    PyObject* decoded_pattern;
    long int_flags;
    char flags[FLAGS_SIZE];
    char check_utf8 = 0;
    const char* pattern_data;
    int pattern_length, flags_length;

    /*
     * Both the builtin re type and our Regex class have attributes
     * "flags" and "pattern".
     */
    py_flags = PyObject_GetAttr(value, _flags_str);
    if (!py_flags) {
        return 0;
    }
    int_flags = PyLong_AsLong(py_flags);
    Py_DECREF(py_flags);
    if (int_flags == -1 && PyErr_Occurred()) {
        return 0;
    }
    py_pattern = PyObject_GetAttr(value, _pattern_str);
    if (!py_pattern) {
        return 0;
    }

    if (PyUnicode_Check(py_pattern)) {
        encoded_pattern = PyUnicode_AsUTF8String(py_pattern);
        Py_DECREF(py_pattern);
        if (!encoded_pattern) {
            return 0;
        }
    } else {
        encoded_pattern = py_pattern;
        check_utf8 = 1;
    }

    if (!(pattern_data = PyBytes_AsString(encoded_pattern))) {
        Py_DECREF(encoded_pattern);
        return 0;
    }
    if ((pattern_length = _downcast_and_check(PyBytes_Size(encoded_pattern), 0)) == -1) {
        Py_DECREF(encoded_pattern);
        return 0;
    }

    if (strlen(pattern_data) != (size_t) pattern_length){
        PyObject* InvalidDocument = _error("InvalidDocument");
        if (InvalidDocument) {
            PyErr_SetString(InvalidDocument,
                            "regex patterns must not contain the NULL byte");
            Py_DECREF(InvalidDocument);
        }
        Py_DECREF(encoded_pattern);
        return 0;
    }

    if (check_utf8) {
        decoded_pattern = PyUnicode_DecodeUTF8(pattern_data, (Py_ssize_t) pattern_length, NULL);
        if (decoded_pattern == NULL) {
            PyErr_Clear();
            PyObject* InvalidStringData = _error("InvalidStringData");
            if (InvalidStringData) {
                PyErr_SetString(InvalidStringData,
                                "regex patterns must be valid UTF-8");
                Py_DECREF(InvalidStringData);
            }
            Py_DECREF(encoded_pattern);
            return 0;
        }
        Py_DECREF(decoded_pattern);
    }

    if (!buffer_write_bytes(buffer, pattern_data, pattern_length + 1)) {
        Py_DECREF(encoded_pattern);
        return 0;
    }
    Py_DECREF(encoded_pattern);

    flags[0] = 0;

    if (int_flags & 2) {
        STRCAT(flags, FLAGS_SIZE, "i");
    }
    if (int_flags & 4) {
        STRCAT(flags, FLAGS_SIZE, "l");
    }
    if (int_flags & 8) {
        STRCAT(flags, FLAGS_SIZE, "m");
    }
    if (int_flags & 16) {
        STRCAT(flags, FLAGS_SIZE, "s");
    }
    if (int_flags & 32) {
        STRCAT(flags, FLAGS_SIZE, "u");
    }
    if (int_flags & 64) {
        STRCAT(flags, FLAGS_SIZE, "x");
    }
    flags_length = (int)strlen(flags) + 1;
    if (!buffer_write_bytes(buffer, flags, flags_length)) {
        return 0;
    }
    *(pymongo_buffer_get_buffer(buffer) + type_byte) = 0x0B;
    return 1;
}

/* Write a single value to the buffer (also write its type_byte, for which
 * space has already been reserved.
 *
 * returns 0 on failure */
static int _write_element_to_buffer(PyObject* self, buffer_t buffer,
                                    int type_byte, PyObject* value,
                                    unsigned char check_keys,
                                    const codec_options_t* options,
                                    unsigned char in_custom_call,
                                    unsigned char in_fallback_call) {
    PyObject* new_value = NULL;
    int retval;
    int is_list;
    long type;
    struct module_state *state = GETSTATE(self);
    if (!state) {
        return 0;
    }
    /*
     * Use _type_marker attribute instead of PyObject_IsInstance for better perf.
     */
    type = _type_marker(value, state->_type_marker_str);
    if (type < 0) {
        return 0;
    }

    switch (type) {
    case 5:
        {
            /* Binary */
            PyObject* subtype_object;
            char subtype;
            const char* data;
            int size;

            *(pymongo_buffer_get_buffer(buffer) + type_byte) = 0x05;
            subtype_object = PyObject_GetAttr(value, state->_subtype_str);
            if (!subtype_object) {
                return 0;
            }
            subtype = (char)PyLong_AsLong(subtype_object);
            if (subtype == -1) {
                Py_DECREF(subtype_object);
                return 0;
            }
            size = _downcast_and_check(PyBytes_Size(value), 0);
            if (size == -1) {
                Py_DECREF(subtype_object);
                return 0;
            }

            Py_DECREF(subtype_object);
            if (subtype == 2) {
                int other_size = _downcast_and_check(PyBytes_Size(value), 4);
                if (other_size == -1)
                    return 0;
                if (!buffer_write_int32(buffer, other_size)) {
                    return 0;
                }
                if (!buffer_write_bytes(buffer, &subtype, 1)) {
                    return 0;
                }
            }
            if (!buffer_write_int32(buffer, size)) {
                return 0;
            }
            if (subtype != 2) {
                if (!buffer_write_bytes(buffer, &subtype, 1)) {
                    return 0;
                }
            }
            data = PyBytes_AsString(value);
            if (!data) {
                return 0;
            }
            if (!buffer_write_bytes(buffer, data, size)) {
                    return 0;
            }
            return 1;
        }
    case 7:
        {
            /* ObjectId */
            const char* data;
            PyObject* pystring = PyObject_GetAttr(value, state->_binary_str);
            if (!pystring) {
                return 0;
            }
            data = PyBytes_AsString(pystring);
            if (!data) {
                Py_DECREF(pystring);
                return 0;
            }
            if (!buffer_write_bytes(buffer, data, 12)) {
                Py_DECREF(pystring);
                return 0;
            }
            Py_DECREF(pystring);
            *(pymongo_buffer_get_buffer(buffer) + type_byte) = 0x07;
            return 1;
        }
    case 9:
        {
            /* DatetimeMS */
            long long millis;
            if (!millis_from_datetime_ms(value, &millis)) {
                return 0;
            }
            *(pymongo_buffer_get_buffer(buffer) + type_byte) = 0x09;
            return buffer_write_int64(buffer, (int64_t)millis);
        }
    case 11:
        {
            /* Regex */
            return _write_regex_to_buffer(buffer, type_byte, value, state->_flags_str, state->_pattern_str);
        }
    case 13:
        {
            /* Code */
            int start_position,
                length_location,
                length;

            PyObject* scope = PyObject_GetAttr(value, state->_scope_str);
            if (!scope) {
                return 0;
            }

            if (scope == Py_None) {
                Py_DECREF(scope);
                *(pymongo_buffer_get_buffer(buffer) + type_byte) = 0x0D;
                return write_string(buffer, value);
            }

            *(pymongo_buffer_get_buffer(buffer) + type_byte) = 0x0F;

            start_position = pymongo_buffer_get_position(buffer);
            /* save space for length */
            length_location = pymongo_buffer_save_space(buffer, 4);
            if (length_location == -1) {
                Py_DECREF(scope);
                return 0;
            }

            if (!write_string(buffer, value)) {
                Py_DECREF(scope);
                return 0;
            }

            if (!write_dict(self, buffer, scope, 0, options, 0)) {
                Py_DECREF(scope);
                return 0;
            }
            Py_DECREF(scope);

            length = pymongo_buffer_get_position(buffer) - start_position;
            buffer_write_int32_at_position(
                buffer, length_location, (int32_t)length);
            return 1;
        }
    case 17:
        {
            /* Timestamp */
            PyObject* obj;
            unsigned long i;

            obj = PyObject_GetAttr(value, state->_inc_str);
            if (!obj) {
                return 0;
            }
            i = PyLong_AsUnsignedLong(obj);
            Py_DECREF(obj);
            if (i == (unsigned long)-1 && PyErr_Occurred()) {
                return 0;
            }
            if (!buffer_write_int32(buffer, (int32_t)i)) {
                return 0;
            }

            obj = PyObject_GetAttr(value, state->_time_str);
            if (!obj) {
                return 0;
            }
            i = PyLong_AsUnsignedLong(obj);
            Py_DECREF(obj);
            if (i == (unsigned long)-1 && PyErr_Occurred()) {
                return 0;
            }
            if (!buffer_write_int32(buffer, (int32_t)i)) {
                return 0;
            }

            *(pymongo_buffer_get_buffer(buffer) + type_byte) = 0x11;
            return 1;
        }
    case 18:
        {
            /* Int64 */
            const long long ll = PyLong_AsLongLong(value);
            if (PyErr_Occurred()) { /* Overflow */
                PyErr_SetString(PyExc_OverflowError,
                                "MongoDB can only handle up to 8-byte ints");
                return 0;
            }
            if (!buffer_write_int64(buffer, (int64_t)ll)) {
                return 0;
            }
            *(pymongo_buffer_get_buffer(buffer) + type_byte) = 0x12;
            return 1;
        }
    case 19:
        {
            /* Decimal128 */
            const char* data;
            PyObject* pystring = PyObject_GetAttr(value, state->_bid_str);
            if (!pystring) {
                return 0;
            }
            data = PyBytes_AsString(pystring);
            if (!data) {
                Py_DECREF(pystring);
                return 0;
            }
            if (!buffer_write_bytes(buffer, data, 16)) {
                Py_DECREF(pystring);
                return 0;
            }
            Py_DECREF(pystring);
            *(pymongo_buffer_get_buffer(buffer) + type_byte) = 0x13;
            return 1;
        }
    case 100:
        {
            /* DBRef */
            PyObject* as_doc = PyObject_CallMethodObjArgs(value, state->_as_doc_str, NULL);
            if (!as_doc) {
                return 0;
            }
            if (!write_dict(self, buffer, as_doc, 0, options, 0)) {
                Py_DECREF(as_doc);
                return 0;
            }
            Py_DECREF(as_doc);
            *(pymongo_buffer_get_buffer(buffer) + type_byte) = 0x03;
            return 1;
        }
    case 101:
        {
            /* RawBSONDocument */
            if (!write_raw_doc(buffer, value, state->_raw_str)) {
                return 0;
            }
            *(pymongo_buffer_get_buffer(buffer) + type_byte) = 0x03;
            return 1;
        }
    case 255:
        {
            /* MinKey */
            *(pymongo_buffer_get_buffer(buffer) + type_byte) = 0xFF;
            return 1;
        }
    case 127:
        {
            /* MaxKey */
            *(pymongo_buffer_get_buffer(buffer) + type_byte) = 0x7F;
            return 1;
        }
    }

    /* No _type_marker attribute or not one of our types. */

    if (PyBool_Check(value)) {
        const char c = (value == Py_True) ? 0x01 : 0x00;
        *(pymongo_buffer_get_buffer(buffer) + type_byte) = 0x08;
        return buffer_write_bytes(buffer, &c, 1);
    }
    else if (PyLong_Check(value)) {
        const long long long_long_value = PyLong_AsLongLong(value);
        if (long_long_value == -1 && PyErr_Occurred()) {
            /* Ignore error and give the fallback_encoder a chance. */
            PyErr_Clear();
        } else if (-2147483648LL <= long_long_value && long_long_value <= 2147483647LL) {
            *(pymongo_buffer_get_buffer(buffer) + type_byte) = 0x10;
            return buffer_write_int32(buffer, (int32_t)long_long_value);
        } else {
            *(pymongo_buffer_get_buffer(buffer) + type_byte) = 0x12;
            return buffer_write_int64(buffer, (int64_t)long_long_value);
        }
    } else if (PyFloat_Check(value)) {
        const double d = PyFloat_AsDouble(value);
        *(pymongo_buffer_get_buffer(buffer) + type_byte) = 0x01;
        return buffer_write_double(buffer, d);
    } else if (value == Py_None) {
        *(pymongo_buffer_get_buffer(buffer) + type_byte) = 0x0A;
        return 1;
    } else if (PyDict_Check(value)) {
        *(pymongo_buffer_get_buffer(buffer) + type_byte) = 0x03;
        return write_dict(self, buffer, value, check_keys, options, 0);
    } else if ((is_list = PyList_Check(value)) || PyTuple_Check(value)) {
        Py_ssize_t items, i;
        int start_position,
            length_location,
            length;
        char zero = 0;

        *(pymongo_buffer_get_buffer(buffer) + type_byte) = 0x04;
        start_position = pymongo_buffer_get_position(buffer);

        /* save space for length */
        length_location = pymongo_buffer_save_space(buffer, 4);
        if (length_location == -1) {
            return 0;
        }
        if (is_list) {
            items = PyList_Size(value);
        } else {
            items = PyTuple_Size(value);
        }
        if (items > BSON_MAX_SIZE) {
            PyObject* BSONError = _error("BSONError");
            if (BSONError) {
                PyErr_SetString(BSONError,
                                "Too many items to serialize.");
                Py_DECREF(BSONError);
            }
            return 0;
        }
        for(i = 0; i < items; i++) {
            int list_type_byte = pymongo_buffer_save_space(buffer, 1);
            char name[BUF_SIZE];
            PyObject* item_value;

            if (list_type_byte == -1) {
                return 0;
            }
            int res = LL2STR(name, (long long)i);
            if (res == -1) {
                return 0;
            }
            if (!buffer_write_bytes(buffer, name, (int)strlen(name) + 1)) {
                return 0;
            }
            if (is_list) {
                item_value = PyList_GET_ITEM(value, i);
            } else {
                item_value = PyTuple_GET_ITEM(value, i);
            }
            if (!item_value) {
                return 0;
            }
            if (!write_element_to_buffer(self, buffer, list_type_byte,
                                         item_value, check_keys, options,
                                         0, 0)) {
                return 0;
            }
        }

        /* write null byte and fill in length */
        if (!buffer_write_bytes(buffer, &zero, 1)) {
            return 0;
        }
        length = pymongo_buffer_get_position(buffer) - start_position;
        buffer_write_int32_at_position(
            buffer, length_location, (int32_t)length);
        return 1;
    /* Python3 special case. Store bytes as BSON binary subtype 0. */
    } else if (PyBytes_Check(value)) {
        char subtype = 0;
        int size;
        const char* data = PyBytes_AS_STRING(value);
        if (!data)
            return 0;
        if ((size = _downcast_and_check(PyBytes_GET_SIZE(value), 0)) == -1)
            return 0;
        *(pymongo_buffer_get_buffer(buffer) + type_byte) = 0x05;
        if (!buffer_write_int32(buffer, (int32_t)size)) {
            return 0;
        }
        if (!buffer_write_bytes(buffer, &subtype, 1)) {
            return 0;
        }
        if (!buffer_write_bytes(buffer, data, size)) {
            return 0;
        }
        return 1;
    } else if (PyUnicode_Check(value)) {
        *(pymongo_buffer_get_buffer(buffer) + type_byte) = 0x02;
        return write_unicode(buffer, value);
    } else if (PyDateTime_Check(value)) {
        long long millis;
        PyObject* utcoffset = PyObject_CallMethodObjArgs(value, state->_utcoffset_str , NULL);
        if (utcoffset == NULL)
            return 0;
        if (utcoffset != Py_None) {
            PyObject* result = PyNumber_Subtract(value, utcoffset);
            Py_DECREF(utcoffset);
            if (!result) {
                return 0;
            }
            millis = millis_from_datetime(result);
            Py_DECREF(result);
        } else {
            millis = millis_from_datetime(value);
        }
        *(pymongo_buffer_get_buffer(buffer) + type_byte) = 0x09;
        return buffer_write_int64(buffer, (int64_t)millis);
    } else if (PyObject_TypeCheck(value, state->REType)) {
        return _write_regex_to_buffer(buffer, type_byte, value, state->_flags_str, state->_pattern_str);
    } else if (PyObject_IsInstance(value, state->Mapping)) {
        /* PyObject_IsInstance returns -1 on error */
        if (PyErr_Occurred()) {
            return 0;
        }
        *(pymongo_buffer_get_buffer(buffer) + type_byte) = 0x03;
        return write_dict(self, buffer, value, check_keys, options, 0);
    } else if (PyObject_IsInstance(value, state->UUID)) {
        PyObject* binary_value = NULL;
        PyObject *uuid_rep_obj = NULL;
        int result;

        /* PyObject_IsInstance returns -1 on error */
        if (PyErr_Occurred()) {
            return 0;
        }

        if (!(uuid_rep_obj = PyLong_FromLong(options->uuid_rep))) {
            return 0;
        }
        binary_value = PyObject_CallMethodObjArgs(state->Binary, state->_from_uuid_str, value, uuid_rep_obj, NULL);
        Py_DECREF(uuid_rep_obj);

        if (binary_value == NULL) {
            return 0;
        }

        result = _write_element_to_buffer(self, buffer,
                                          type_byte, binary_value,
                                          check_keys, options,
                                          in_custom_call,
                                          in_fallback_call);
        Py_DECREF(binary_value);
        return result;
    }

    /* Try a custom encoder if one is provided and we have not already
     * attempted to use a type encoder. */
    if (!in_custom_call && !options->type_registry.is_encoder_empty) {
        PyObject* value_type = NULL;
        PyObject* converter = NULL;
        value_type = PyObject_Type(value);
        if (value_type == NULL) {
            return 0;
        }
        converter = PyDict_GetItem(options->type_registry.encoder_map, value_type);
        Py_XDECREF(value_type);
        if (converter != NULL) {
            /* Transform types that have a registered converter.
             * A new reference is created upon transformation. */
            new_value = PyObject_CallFunctionObjArgs(converter, value, NULL);
            if (new_value == NULL) {
                return 0;
            }
            retval = write_element_to_buffer(self, buffer, type_byte, new_value,
                                             check_keys, options, 1, 0);
            Py_XDECREF(new_value);
            return retval;
        }
    }

    /* Try the fallback encoder if one is provided and we have not already
     * attempted to use the fallback encoder. */
    if (!in_fallback_call && options->type_registry.has_fallback_encoder) {
        new_value = PyObject_CallFunctionObjArgs(
            options->type_registry.fallback_encoder, value, NULL);
        if (new_value == NULL) {
            // propagate any exception raised by the callback
            return 0;
        }
        retval = write_element_to_buffer(self, buffer, type_byte, new_value,
                                         check_keys, options, 0, 1);
        Py_XDECREF(new_value);
        return retval;
    }

    /* We can't determine value's type. Fail. */
    _set_cannot_encode(value);
    return 0;
}

static int check_key_name(const char* name, int name_length) {

    if (name_length > 0 && name[0] == '$') {
        PyObject* InvalidDocument = _error("InvalidDocument");
        if (InvalidDocument) {
            PyObject* errmsg = PyUnicode_FromFormat(
                    "key '%s' must not start with '$'", name);
            if (errmsg) {
                PyErr_SetObject(InvalidDocument, errmsg);
                Py_DECREF(errmsg);
            }
            Py_DECREF(InvalidDocument);
        }
        return 0;
    }
    if (strchr(name, '.')) {
        PyObject* InvalidDocument = _error("InvalidDocument");
        if (InvalidDocument) {
            PyObject* errmsg = PyUnicode_FromFormat(
                    "key '%s' must not contain '.'", name);
            if (errmsg) {
                PyErr_SetObject(InvalidDocument, errmsg);
                Py_DECREF(errmsg);
            }
            Py_DECREF(InvalidDocument);
        }
        return 0;
    }
    return 1;
}

/* Write a (key, value) pair to the buffer.
 *
 * Returns 0 on failure */
int write_pair(PyObject* self, buffer_t buffer, const char* name, int name_length,
               PyObject* value, unsigned char check_keys,
               const codec_options_t* options, unsigned char allow_id) {
    int type_byte;

    /* Don't write any _id elements unless we're explicitly told to -
     * _id has to be written first so we do so, but don't bother
     * deleting it from the dictionary being written. */
    if (!allow_id && strcmp(name, "_id") == 0) {
        return 1;
    }

    type_byte = pymongo_buffer_save_space(buffer, 1);
    if (type_byte == -1) {
        return 0;
    }
    if (check_keys && !check_key_name(name, name_length)) {
        return 0;
    }
    if (!buffer_write_bytes(buffer, name, name_length + 1)) {
        return 0;
    }
    if (!write_element_to_buffer(self, buffer, type_byte,
                                 value, check_keys, options, 0, 0)) {
        return 0;
    }
    return 1;
}

int decode_and_write_pair(PyObject* self, buffer_t buffer,
                          PyObject* key, PyObject* value,
                          unsigned char check_keys,
                          const codec_options_t* options,
                          unsigned char top_level) {
    PyObject* encoded;
    const char* data;
    int size;
    if (PyUnicode_Check(key)) {
        encoded = PyUnicode_AsUTF8String(key);
        if (!encoded) {
            return 0;
        }
        if (!(data = PyBytes_AS_STRING(encoded))) {
            Py_DECREF(encoded);
            return 0;
        }
        if ((size = _downcast_and_check(PyBytes_GET_SIZE(encoded), 1)) == -1) {
            Py_DECREF(encoded);
            return 0;
        }
        if (strlen(data) != (size_t)(size - 1)) {
            PyObject* InvalidDocument = _error("InvalidDocument");
            if (InvalidDocument) {
                PyErr_SetString(InvalidDocument,
                                "Key names must not contain the NULL byte");
                Py_DECREF(InvalidDocument);
            }
            Py_DECREF(encoded);
            return 0;
        }
    } else {
        PyObject* InvalidDocument = _error("InvalidDocument");
        if (InvalidDocument) {
            PyObject* repr = PyObject_Repr(key);
            if (repr) {
                PyObject* errmsg = PyUnicode_FromString(
                        "documents must have only string keys, key was ");
                if (errmsg) {
                    PyObject* error = PyUnicode_Concat(errmsg, repr);
                    if (error) {
                        PyErr_SetObject(InvalidDocument, error);
                        Py_DECREF(error);
                    }
                    Py_DECREF(errmsg);
                    Py_DECREF(repr);
                } else {
                    Py_DECREF(repr);
                }
            }
            Py_DECREF(InvalidDocument);
        }
        return 0;
    }

    /* If top_level is True, don't allow writing _id here - it was already written. */
    if (!write_pair(self, buffer, data,
                    size - 1, value, check_keys, options, !top_level)) {
        Py_DECREF(encoded);
        return 0;
    }

    Py_DECREF(encoded);
    return 1;
}


/* Write a RawBSONDocument to the buffer.
 * Returns the number of bytes written or 0 on failure.
 */
static int write_raw_doc(buffer_t buffer, PyObject* raw, PyObject* _raw_str) {
    char* bytes;
    Py_ssize_t len;
    int len_int;
    int bytes_written = 0;
    PyObject* bytes_obj = NULL;

    bytes_obj = PyObject_GetAttr(raw, _raw_str);
    if (!bytes_obj) {
        goto fail;
    }

    if (-1 == PyBytes_AsStringAndSize(bytes_obj, &bytes, &len)) {
        goto fail;
    }
    len_int = _downcast_and_check(len, 0);
    if (-1 == len_int) {
        goto fail;
    }
    if (!buffer_write_bytes(buffer, bytes, len_int)) {
        goto fail;
    }
    bytes_written = len_int;
fail:
    Py_XDECREF(bytes_obj);
    return bytes_written;
}

/* returns the number of bytes written or 0 on failure */
int write_dict(PyObject* self, buffer_t buffer,
               PyObject* dict, unsigned char check_keys,
               const codec_options_t* options, unsigned char top_level) {
    PyObject* key;
    PyObject* iter;
    char zero = 0;
    int length;
    int length_location;
    struct module_state *state = GETSTATE(self);
    long type_marker;
    int is_dict = PyDict_Check(dict);
    if (!state) {
        return 0;
    }

    if (!is_dict) {
        /* check for RawBSONDocument */
        type_marker = _type_marker(dict, state->_type_marker_str);
        if (type_marker < 0) {
            return 0;
        }

        if (101 == type_marker) {
            return write_raw_doc(buffer, dict, state->_raw_str);
        }

        if (!PyObject_IsInstance(dict, state->Mapping)) {
            PyObject* repr;
            if ((repr = PyObject_Repr(dict))) {
                PyObject* errmsg = PyUnicode_FromString(
                    "encoder expected a mapping type but got: ");
                if (errmsg) {
                    PyObject* error = PyUnicode_Concat(errmsg, repr);
                    if (error) {
                        PyErr_SetObject(PyExc_TypeError, error);
                        Py_DECREF(error);
                    }
                    Py_DECREF(errmsg);
                    Py_DECREF(repr);
                }
                else {
                    Py_DECREF(repr);
                }
            } else {
                PyErr_SetString(PyExc_TypeError,
                                "encoder expected a mapping type");
            }

            return 0;
        }
        /* PyObject_IsInstance returns -1 on error */
        if (PyErr_Occurred()) {
            return 0;
        }
    }

    length_location = pymongo_buffer_save_space(buffer, 4);
    if (length_location == -1) {
        return 0;
    }

    /* Write _id first if this is a top level doc. */
    if (top_level) {
        /*
         * If "dict" is a defaultdict we don't want to call
         * PyObject_GetItem on it. That would **create**
         * an _id where one didn't previously exist (PYTHON-871).
         */
        if (is_dict) {
            /* PyDict_GetItem returns a borrowed reference. */
            PyObject* _id = PyDict_GetItem(dict, state->_id_str);
            if (_id) {
                if (!write_pair(self, buffer, "_id", 3,
                                _id, check_keys, options, 1)) {
                    return 0;
                }
            }
        } else if (PyMapping_HasKey(dict, state->_id_str)) {
            PyObject* _id = PyObject_GetItem(dict, state->_id_str);
            if (!_id) {
                return 0;
            }
            if (!write_pair(self, buffer, "_id", 3,
                            _id, check_keys, options, 1)) {
                Py_DECREF(_id);
                return 0;
            }
            /* PyObject_GetItem returns a new reference. */
            Py_DECREF(_id);
        }
    }

    if (is_dict) {
        PyObject* value;
        Py_ssize_t pos = 0;
        while (PyDict_Next(dict, &pos, &key, &value)) {
            if (!decode_and_write_pair(self, buffer, key, value,
                                    check_keys, options, top_level)) {
                return 0;
            }
        }
    } else {
        iter = PyObject_GetIter(dict);
        if (iter == NULL) {
            return 0;
        }
        while ((key = PyIter_Next(iter)) != NULL) {
            PyObject* value = PyObject_GetItem(dict, key);
            if (!value) {
                PyErr_SetObject(PyExc_KeyError, key);
                Py_DECREF(key);
                Py_DECREF(iter);
                return 0;
            }
            if (!decode_and_write_pair(self, buffer, key, value,
                                    check_keys, options, top_level)) {
                Py_DECREF(key);
                Py_DECREF(value);
                Py_DECREF(iter);
                return 0;
            }
            Py_DECREF(key);
            Py_DECREF(value);
        }
        Py_DECREF(iter);
        if (PyErr_Occurred()) {
            return 0;
        }
    }

    /* write null byte and fill in length */
    if (!buffer_write_bytes(buffer, &zero, 1)) {
        return 0;
    }
    length = pymongo_buffer_get_position(buffer) - length_location;
    buffer_write_int32_at_position(
        buffer, length_location, (int32_t)length);
    return length;
}

static PyObject* _cbson_dict_to_bson(PyObject* self, PyObject* args) {
    PyObject* dict;
    PyObject* result;
    unsigned char check_keys;
    unsigned char top_level = 1;
    PyObject* options_obj;
    codec_options_t options;
    buffer_t buffer;
    PyObject* raw_bson_document_bytes_obj;
    long type_marker;
    struct module_state *state = GETSTATE(self);
    if (!state) {
        return NULL;
    }

    if (!(PyArg_ParseTuple(args, "ObO|b", &dict, &check_keys,
                          &options_obj, &top_level) &&
            convert_codec_options(self, options_obj, &options))) {
        return NULL;
    }

    /* check for RawBSONDocument */
    type_marker = _type_marker(dict, state->_type_marker_str);
    if (type_marker < 0) {
        destroy_codec_options(&options);
        return NULL;
    } else if (101 == type_marker) {
        destroy_codec_options(&options);
        raw_bson_document_bytes_obj = PyObject_GetAttr(dict, state->_raw_str);
        if (NULL == raw_bson_document_bytes_obj) {
            return NULL;
        }
        return raw_bson_document_bytes_obj;
    }

    buffer = pymongo_buffer_new();
    if (!buffer) {
        destroy_codec_options(&options);
        return NULL;
    }

    if (!write_dict(self, buffer, dict, check_keys, &options, top_level)) {
        destroy_codec_options(&options);
        pymongo_buffer_free(buffer);
        return NULL;
    }

    /* objectify buffer */
    result = Py_BuildValue("y#", pymongo_buffer_get_buffer(buffer),
                           (Py_ssize_t)pymongo_buffer_get_position(buffer));
    destroy_codec_options(&options);
    pymongo_buffer_free(buffer);
    return result;
}

/*
 * Hook for optional decoding BSON documents to DBRef.
 */
static PyObject *_dbref_hook(PyObject* self, PyObject* value) {
    struct module_state *state = GETSTATE(self);
    PyObject* ref = NULL;
    PyObject* id = NULL;
    PyObject* database = NULL;
    PyObject* ret = NULL;
    int db_present = 0;
    if (!state) {
        return NULL;
    }

    /* Decoding for DBRefs */
    if (PyMapping_HasKey(value, state->_dollar_ref_str) && PyMapping_HasKey(value, state->_dollar_id_str)) { /* DBRef */
        ref = PyObject_GetItem(value, state->_dollar_ref_str);
        /* PyObject_GetItem returns NULL to indicate error. */
        if (!ref) {
            goto invalid;
        }
        id = PyObject_GetItem(value, state->_dollar_id_str);
        /* PyObject_GetItem returns NULL to indicate error. */
        if (!id) {
            goto invalid;
        }

        if (PyMapping_HasKey(value, state->_dollar_db_str)) {
            database = PyObject_GetItem(value, state->_dollar_db_str);
            if (!database) {
                goto invalid;
            }
            db_present = 1;
        } else {
            database = Py_None;
            Py_INCREF(database);
        }

        // check types
        if (!(PyUnicode_Check(ref) && (database == Py_None || PyUnicode_Check(database)))) {
            ret = value;
            goto invalid;
        }

        PyMapping_DelItem(value, state->_dollar_ref_str);
        PyMapping_DelItem(value, state->_dollar_id_str);
        if (db_present) {
            PyMapping_DelItem(value, state->_dollar_db_str);
        }

        ret = PyObject_CallFunctionObjArgs(state->DBRef, ref, id, database, value, NULL);
        Py_DECREF(value);
    } else {
        ret = value;
    }
invalid:
    Py_XDECREF(ref);
    Py_XDECREF(id);
    Py_XDECREF(database);
    return ret;
}

static PyObject* get_value(PyObject* self, PyObject* name, const char* buffer,
                           unsigned* position, unsigned char type,
                           unsigned max, const codec_options_t* options, int raw_array) {
    struct module_state *state = GETSTATE(self);
    PyObject* value = NULL;
    if (!state) {
        return NULL;
    }
    switch (type) {
    case 1:
        {
            double d;
            if (max < 8) {
                goto invalid;
            }
            memcpy(&d, buffer + *position, 8);
            value = PyFloat_FromDouble(BSON_DOUBLE_FROM_LE(d));
            *position += 8;
            break;
        }
    case 2:
    case 14:
        {
            uint32_t value_length;
            if (max < 4) {
                goto invalid;
            }
            memcpy(&value_length, buffer + *position, 4);
            value_length = BSON_UINT32_FROM_LE(value_length);
            /* Encoded string length + string */
            if (!value_length || max < value_length || max < 4 + value_length) {
                goto invalid;
            }
            *position += 4;
            /* Strings must end in \0 */
            if (buffer[*position + value_length - 1]) {
                goto invalid;
            }
            value = PyUnicode_DecodeUTF8(
                buffer + *position, value_length - 1,
                options->unicode_decode_error_handler);
            if (!value) {
                goto invalid;
            }
            *position += value_length;
            break;
        }
    case 3:
        {
            uint32_t size;

            if (max < 4) {
                goto invalid;
            }
            memcpy(&size, buffer + *position, 4);
            size = BSON_UINT32_FROM_LE(size);
            if (size < BSON_MIN_SIZE || max < size) {
                goto invalid;
            }
            /* Check for bad eoo */
            if (buffer[*position + size - 1]) {
                goto invalid;
            }

            value = elements_to_dict(self, buffer + *position,
                                     size, options);
            if (!value) {
                goto invalid;
            }

            if (options->is_raw_bson) {
                *position += size;
                break;
            }

            /* Hook for DBRefs */
            value = _dbref_hook(self, value);
            if (!value) {
                goto invalid;
            }

            *position += size;
            break;
        }
    case 4:
        {
            uint32_t size, end;

            if (max < 4) {
                goto invalid;
            }
            memcpy(&size, buffer + *position, 4);
            size = BSON_UINT32_FROM_LE(size);
            if (size < BSON_MIN_SIZE || max < size) {
                goto invalid;
            }

            end = *position + size - 1;
            /* Check for bad eoo */
            if (buffer[end]) {
                goto invalid;
            }

            if (raw_array != 0) {
                // Treat it as a binary buffer.
                value = PyBytes_FromStringAndSize(buffer + *position, size);
                *position += size;
                break;
            }

            *position += 4;

            value = PyList_New(0);
            if (!value) {
                goto invalid;
            }
            while (*position < end) {
                PyObject* to_append;

                unsigned char bson_type = (unsigned char)buffer[(*position)++];

                size_t key_size = strlen(buffer + *position);
                if (max < key_size) {
                    Py_DECREF(value);
                    goto invalid;
                }
                /* just skip the key, they're in order. */
                *position += (unsigned)key_size + 1;
                if (Py_EnterRecursiveCall(" while decoding a list value")) {
                    Py_DECREF(value);
                    goto invalid;
                }
                to_append = get_value(self, name, buffer, position, bson_type,
                                      max - (unsigned)key_size, options, raw_array);
                Py_LeaveRecursiveCall();
                if (!to_append) {
                    Py_DECREF(value);
                    goto invalid;
                }
                if (PyList_Append(value, to_append) < 0) {
                    Py_DECREF(value);
                    Py_DECREF(to_append);
                    goto invalid;
                }
                Py_DECREF(to_append);
            }
            if (*position != end) {
                goto invalid;
            }
            (*position)++;
            break;
        }
    case 5:
        {
            PyObject* data;
            PyObject* st;
            uint32_t length, length2;
            unsigned char subtype;

            if (max < 5) {
                goto invalid;
            }
            memcpy(&length, buffer + *position, 4);
            length = BSON_UINT32_FROM_LE(length);
            if (max < length) {
                goto invalid;
            }

            subtype = (unsigned char)buffer[*position + 4];
            *position += 5;
            if (subtype == 2) {
                if (length < 4) {
                    goto invalid;
                }
                memcpy(&length2, buffer + *position, 4);
                length2 = BSON_UINT32_FROM_LE(length2);
                if (length2 != length - 4) {
                    goto invalid;
                }
            }
            /* Python3 special case. Decode BSON binary subtype 0 to bytes. */
            if (subtype == 0) {
                value = PyBytes_FromStringAndSize(buffer + *position, length);
                *position += length;
                break;
            }
            if (subtype == 2) {
                data = PyBytes_FromStringAndSize(buffer + *position + 4, length - 4);
            } else {
                data = PyBytes_FromStringAndSize(buffer + *position, length);
            }
            if (!data) {
                goto invalid;
            }
            /* Encode as UUID or Binary based on options->uuid_rep */
            if (subtype == 3 || subtype == 4) {
                PyObject* binary_value = NULL;
                char uuid_rep = options->uuid_rep;

                /* UUID should always be 16 bytes */
                if (length != 16) {
                    goto uuiderror;
                }

                binary_value = PyObject_CallFunction(state->Binary, "(Oi)", data, subtype);
                if (binary_value == NULL) {
                    goto uuiderror;
                }

                if ((uuid_rep == UNSPECIFIED) ||
                        (subtype == 4 && uuid_rep != STANDARD) ||
                        (subtype == 3 && uuid_rep == STANDARD)) {
                    value = binary_value;
                    Py_INCREF(value);
                } else {
                    PyObject *uuid_rep_obj = PyLong_FromLong(uuid_rep);
                    if (!uuid_rep_obj) {
                        goto uuiderror;
                    }
                    value = PyObject_CallMethodObjArgs(binary_value, state->_as_uuid_str, uuid_rep_obj, NULL);
                    Py_DECREF(uuid_rep_obj);
                }

            uuiderror:
                Py_XDECREF(binary_value);
                Py_DECREF(data);
                if (!value) {
                    goto invalid;
                }
                *position += length;
                break;
            }

            st = PyLong_FromLong(subtype);
            if (!st) {
                Py_DECREF(data);
                goto invalid;
            }
            value = PyObject_CallFunctionObjArgs(state->Binary, data, st, NULL);
            Py_DECREF(st);
            Py_DECREF(data);
            if (!value) {
                goto invalid;
            }
            *position += length;
            break;
        }
    case 6:
    case 10:
        {
            value = Py_None;
            Py_INCREF(value);
            break;
        }
    case 7:
        {
            if (max < 12) {
                goto invalid;
            }
            value = PyObject_CallFunction(state->ObjectId, "y#", buffer + *position, (Py_ssize_t)12);
            *position += 12;
            break;
        }
    case 8:
        {
            char boolean_raw = buffer[(*position)++];
            if (0 == boolean_raw) {
                value = Py_False;
            } else if (1 == boolean_raw) {
                value = Py_True;
            } else {
                PyObject* InvalidBSON = _error("InvalidBSON");
                if (InvalidBSON) {
                    PyErr_Format(InvalidBSON, "invalid boolean value: %x", boolean_raw);
                    Py_DECREF(InvalidBSON);
                }
                return NULL;
            }
            Py_INCREF(value);
            break;
        }
    case 9:
        {
            PyObject* naive;
            PyObject* replace;
            PyObject* args;
            PyObject* kwargs;
            PyObject* astimezone;
            int64_t millis;
            if (max < 8) {
                goto invalid;
            }
            memcpy(&millis, buffer + *position, 8);
            millis = (int64_t)BSON_UINT64_FROM_LE(millis);
            *position += 8;

            if (options->datetime_conversion == DATETIME_MS){
                value = datetime_ms_from_millis(self, millis);
                break;
            }

            int dt_clamp = options->datetime_conversion == DATETIME_CLAMP;
            int dt_auto = options->datetime_conversion == DATETIME_AUTO;


            if (dt_clamp || dt_auto){
                PyObject *min_millis_fn_res;
                PyObject *max_millis_fn_res;
                int64_t min_millis;
                int64_t max_millis;

                if (options->tz_aware){
                    PyObject* tzinfo = options->tzinfo;
                    if (tzinfo == Py_None) {
                        // Default to UTC.
                        tzinfo = state->UTC;
                    }
                    min_millis_fn_res = PyObject_CallFunctionObjArgs(state->_min_datetime_ms, tzinfo, NULL);
                    max_millis_fn_res = PyObject_CallFunctionObjArgs(state->_max_datetime_ms, tzinfo, NULL);
                } else {
                    min_millis_fn_res = PyObject_CallObject(state->_min_datetime_ms, NULL);
                    max_millis_fn_res = PyObject_CallObject(state->_max_datetime_ms, NULL);
                }

                if (!min_millis_fn_res || !max_millis_fn_res){
                    Py_XDECREF(min_millis_fn_res);
                    Py_XDECREF(max_millis_fn_res);
                    goto invalid;
                }

                min_millis = PyLong_AsLongLong(min_millis_fn_res);
                max_millis = PyLong_AsLongLong(max_millis_fn_res);

                if ((min_millis == -1 || max_millis == -1) && PyErr_Occurred())
                {
                    // min/max_millis check
                    goto invalid;
                }

                if (dt_clamp) {
                    if (millis < min_millis) {
                        millis = min_millis;
                    } else if (millis > max_millis) {
                        millis = max_millis;
                    }
                    // Continues from here to return a datetime.
                } else { // dt_auto
                    if (millis < min_millis || millis > max_millis){
                        value = datetime_ms_from_millis(self, millis);
                        break; // Out-of-range so done.
                    }
                }
            }

            naive = datetime_from_millis(millis);
            if (!options->tz_aware) { /* In the naive case, we're done here. */
                value = naive;
                break;
            }

            if (!naive) {
                goto invalid;
            }
            replace = PyObject_GetAttr(naive, state->_replace_str);
            Py_DECREF(naive);
            if (!replace) {
                goto invalid;
            }
            args = PyTuple_New(0);
            if (!args) {
                Py_DECREF(replace);
                goto invalid;
            }
            kwargs = PyDict_New();
            if (!kwargs) {
                Py_DECREF(replace);
                Py_DECREF(args);
                goto invalid;
            }
            if (PyDict_SetItem(kwargs, state->_tzinfo_str, state->UTC) == -1) {
                Py_DECREF(replace);
                Py_DECREF(args);
                Py_DECREF(kwargs);
                goto invalid;
            }
            value = PyObject_Call(replace, args, kwargs);
            if (!value) {
                Py_DECREF(replace);
                Py_DECREF(args);
                Py_DECREF(kwargs);
                goto invalid;
            }

            /* convert to local time */
            if (options->tzinfo != Py_None) {
                astimezone = PyObject_GetAttr(value, state->_astimezone_str);
                Py_DECREF(value);
                if (!astimezone) {
                    Py_DECREF(replace);
                    Py_DECREF(args);
                    Py_DECREF(kwargs);
                    goto invalid;
                }
                value = PyObject_CallFunctionObjArgs(astimezone, options->tzinfo, NULL);
                Py_DECREF(astimezone);
            }

            Py_DECREF(replace);
            Py_DECREF(args);
            Py_DECREF(kwargs);
            break;
        }
    case 11:
        {
            PyObject* pattern;
            int flags;
            size_t flags_length, i;
            size_t pattern_length = strlen(buffer + *position);
            if (pattern_length > BSON_MAX_SIZE || max < pattern_length) {
                goto invalid;
            }
            pattern = PyUnicode_DecodeUTF8(
                buffer + *position, pattern_length,
                options->unicode_decode_error_handler);
            if (!pattern) {
                goto invalid;
            }
            *position += (unsigned)pattern_length + 1;
            flags_length = strlen(buffer + *position);
            if (flags_length > BSON_MAX_SIZE ||
                    (BSON_MAX_SIZE - pattern_length) < flags_length) {
                Py_DECREF(pattern);
                goto invalid;
            }
            if (max < pattern_length + flags_length) {
                Py_DECREF(pattern);
                goto invalid;
            }
            flags = 0;
            for (i = 0; i < flags_length; i++) {
                if (buffer[*position + i] == 'i') {
                    flags |= 2;
                } else if (buffer[*position + i] == 'l') {
                    flags |= 4;
                } else if (buffer[*position + i] == 'm') {
                    flags |= 8;
                } else if (buffer[*position + i] == 's') {
                    flags |= 16;
                } else if (buffer[*position + i] == 'u') {
                    flags |= 32;
                } else if (buffer[*position + i] == 'x') {
                    flags |= 64;
                }
            }
            *position += (unsigned)flags_length + 1;

            value = PyObject_CallFunction(state->Regex, "Oi", pattern, flags);
            Py_DECREF(pattern);
            break;
        }
    case 12:
        {
            uint32_t coll_length;
            PyObject* collection;
            PyObject* id = NULL;

            if (max < 4) {
                goto invalid;
            }
            memcpy(&coll_length, buffer + *position, 4);
            coll_length = BSON_UINT32_FROM_LE(coll_length);
            /* Encoded string length + string + 12 byte ObjectId */
            if (!coll_length || max < coll_length || max < 4 + coll_length + 12) {
                goto invalid;
            }
            *position += 4;
            /* Strings must end in \0 */
            if (buffer[*position + coll_length - 1]) {
                goto invalid;
            }

            collection = PyUnicode_DecodeUTF8(
                buffer + *position, coll_length - 1,
                options->unicode_decode_error_handler);
            if (!collection) {
                goto invalid;
            }
            *position += coll_length;

            id = PyObject_CallFunction(state->ObjectId, "y#", buffer + *position, (Py_ssize_t)12);
            if (!id) {
                Py_DECREF(collection);
                goto invalid;
            }
            *position += 12;
            value = PyObject_CallFunctionObjArgs(state->DBRef, collection, id, NULL);
            Py_DECREF(collection);
            Py_DECREF(id);
            break;
        }
    case 13:
        {
            PyObject* code;
            uint32_t value_length;
            if (max < 4) {
                goto invalid;
            }
            memcpy(&value_length, buffer + *position, 4);
            value_length = BSON_UINT32_FROM_LE(value_length);
            /* Encoded string length + string */
            if (!value_length || max < value_length || max < 4 + value_length) {
                goto invalid;
            }
            *position += 4;
            /* Strings must end in \0 */
            if (buffer[*position + value_length - 1]) {
                goto invalid;
            }
            code = PyUnicode_DecodeUTF8(
                buffer + *position, value_length - 1,
                options->unicode_decode_error_handler);
            if (!code) {
                goto invalid;
            }
            *position += value_length;
            value = PyObject_CallFunctionObjArgs(state->Code, code, NULL, NULL);
            Py_DECREF(code);
            break;
        }
    case 15:
        {
            uint32_t c_w_s_size;
            uint32_t code_size;
            uint32_t scope_size;
            uint32_t len;
            PyObject* code;
            PyObject* scope;

            if (max < 8) {
                goto invalid;
            }

            memcpy(&c_w_s_size, buffer + *position, 4);
            c_w_s_size = BSON_UINT32_FROM_LE(c_w_s_size);
            *position += 4;

            if (max < c_w_s_size) {
                goto invalid;
            }

            memcpy(&code_size, buffer + *position, 4);
            code_size = BSON_UINT32_FROM_LE(code_size);
            /* code_w_scope length + code length + code + scope length */
            len = 4 + 4 + code_size + 4;
            if (!code_size || max < code_size || max < len || len < code_size) {
                goto invalid;
            }
            *position += 4;
            /* Strings must end in \0 */
            if (buffer[*position + code_size - 1]) {
                goto invalid;
            }
            code = PyUnicode_DecodeUTF8(
                buffer + *position, code_size - 1,
                options->unicode_decode_error_handler);
            if (!code) {
                goto invalid;
            }
            *position += code_size;

            memcpy(&scope_size, buffer + *position, 4);
            scope_size = BSON_UINT32_FROM_LE(scope_size);
            /* code length + code + scope length + scope */
            len = 4 + 4 + code_size + scope_size;
            if (scope_size < BSON_MIN_SIZE || len != c_w_s_size || len < scope_size) {
                Py_DECREF(code);
                goto invalid;
            }

            /* Check for bad eoo */
            if (buffer[*position + scope_size - 1]) {
                goto invalid;
            }
            scope = elements_to_dict(self, buffer + *position,
                                 scope_size, options);
            if (!scope) {
                Py_DECREF(code);
                goto invalid;
            }
            *position += scope_size;

            value = PyObject_CallFunctionObjArgs(state->Code, code, scope, NULL);
            Py_DECREF(code);
            Py_DECREF(scope);
            break;
        }
    case 16:
        {
            int32_t i;
            if (max < 4) {
                goto invalid;
            }
            memcpy(&i, buffer + *position, 4);
            i = (int32_t)BSON_UINT32_FROM_LE(i);
            value = PyLong_FromLong(i);
            if (!value) {
                goto invalid;
            }
            *position += 4;
            break;
        }
    case 17:
        {
            uint32_t time, inc;
            if (max < 8) {
                goto invalid;
            }
            memcpy(&inc, buffer + *position, 4);
            memcpy(&time, buffer + *position + 4, 4);
            inc = BSON_UINT32_FROM_LE(inc);
            time = BSON_UINT32_FROM_LE(time);
            value = PyObject_CallFunction(state->Timestamp, "II", time, inc);
            *position += 8;
            break;
        }
    case 18:
        {
            int64_t ll;
            if (max < 8) {
                goto invalid;
            }
            memcpy(&ll, buffer + *position, 8);
            ll = (int64_t)BSON_UINT64_FROM_LE(ll);
            value = PyObject_CallFunction(state->BSONInt64, "L", ll);
            *position += 8;
            break;
        }
    case 19:
        {
            if (max < 16) {
                goto invalid;
            }
            PyObject *_bytes_obj = PyBytes_FromStringAndSize(buffer + *position, (Py_ssize_t)16);
            if (!_bytes_obj) {
                goto invalid;
            }
            value = PyObject_CallMethodObjArgs(state->Decimal128, state->_from_bid_str, _bytes_obj, NULL);
            Py_DECREF(_bytes_obj);
            *position += 16;
            break;
        }
    case 255:
        {
            value = PyObject_CallFunctionObjArgs(state->MinKey, NULL);
            break;
        }
    case 127:
        {
            value = PyObject_CallFunctionObjArgs(state->MaxKey, NULL);
            break;
        }
    default:
        {
            PyObject* InvalidBSON = _error("InvalidBSON");
            if (InvalidBSON) {
                PyObject* bobj = PyBytes_FromFormat("%c", type);
                if (bobj) {
                    PyObject* repr = PyObject_Repr(bobj);
                    Py_DECREF(bobj);
                    /*
                     * See http://bugs.python.org/issue22023 for why we can't
                     * just use PyUnicode_FromFormat with %S or %R to do this
                     * work.
                     */
                    if (repr) {
                        PyObject* left = PyUnicode_FromString(
                            "Detected unknown BSON type ");
                        if (left) {
                            PyObject* lmsg = PyUnicode_Concat(left, repr);
                            Py_DECREF(left);
                            if (lmsg) {
                                PyObject* errmsg = PyUnicode_FromFormat(
                                    "%U for fieldname '%U'. Are you using the "
                                    "latest driver version?", lmsg, name);
                                if (errmsg) {
                                    PyErr_SetObject(InvalidBSON, errmsg);
                                    Py_DECREF(errmsg);
                                }
                                Py_DECREF(lmsg);
                            }
                        }
                        Py_DECREF(repr);
                    }
                }
                Py_DECREF(InvalidBSON);
            }
            goto invalid;
        }
    }

    if (value) {
        if (!options->type_registry.is_decoder_empty) {
            PyObject* value_type = NULL;
            PyObject* converter = NULL;
            value_type = PyObject_Type(value);
            if (value_type == NULL) {
                goto invalid;
            }
            converter = PyDict_GetItem(options->type_registry.decoder_map, value_type);
            if (converter != NULL) {
                PyObject* new_value = PyObject_CallFunctionObjArgs(converter, value, NULL);
                Py_DECREF(value_type);
                Py_DECREF(value);
                return new_value;
            } else {
                Py_DECREF(value_type);
                return value;
            }
        }
        return value;
    }

    invalid:

    /*
     * Wrap any non-InvalidBSON errors in InvalidBSON.
     */
    if (PyErr_Occurred()) {
        PyObject *etype, *evalue, *etrace;
        PyObject *InvalidBSON;

        /*
         * Calling _error clears the error state, so fetch it first.
         */
        PyErr_Fetch(&etype, &evalue, &etrace);

        /* Dont reraise anything but PyExc_Exceptions as InvalidBSON. */
        if (PyErr_GivenExceptionMatches(etype, PyExc_Exception)) {
            InvalidBSON = _error("InvalidBSON");
            if (InvalidBSON) {
                if (!PyErr_GivenExceptionMatches(etype, InvalidBSON)) {
                    /*
                     * Raise InvalidBSON(str(e)).
                     */
                    Py_DECREF(etype);
                    etype = InvalidBSON;

                    if (evalue) {
                        PyObject *msg = PyObject_Str(evalue);
                        Py_DECREF(evalue);
                        evalue = msg;
                    }
                    PyErr_NormalizeException(&etype, &evalue, &etrace);
                } else {
                    /*
                     * The current exception matches InvalidBSON, so we don't
                     * need this reference after all.
                     */
                    Py_DECREF(InvalidBSON);
                }
            }
        }
        /* Steals references to args. */
        PyErr_Restore(etype, evalue, etrace);
    } else {
        PyObject *InvalidBSON = _error("InvalidBSON");
        if (InvalidBSON) {
            PyErr_SetString(InvalidBSON, "invalid length or type code");
            Py_DECREF(InvalidBSON);
        }
    }
    return NULL;
}

/*
 * Get the next 'name' and 'value' from a document in a string, whose position
 * is provided.
 *
 * Returns the position of the next element in the document, or -1 on error.
 */
static int _element_to_dict(PyObject* self, const char* string,
                            unsigned position, unsigned max,
                            const codec_options_t* options,
                            int raw_array,
                            PyObject** name, PyObject** value) {
    unsigned char type = (unsigned char)string[position++];
    size_t name_length = strlen(string + position);
    if (name_length > BSON_MAX_SIZE || position + name_length >= max) {
        PyObject* InvalidBSON = _error("InvalidBSON");
        if (InvalidBSON) {
            PyErr_SetString(InvalidBSON, "field name too large");
            Py_DECREF(InvalidBSON);
        }
        return -1;
    }
    *name = PyUnicode_DecodeUTF8(
        string + position, name_length,
        options->unicode_decode_error_handler);
    if (!*name) {
        /* If NULL is returned then wrap the UnicodeDecodeError
           in an InvalidBSON error */
        PyObject *etype, *evalue, *etrace;
        PyObject *InvalidBSON;

        PyErr_Fetch(&etype, &evalue, &etrace);
        if (PyErr_GivenExceptionMatches(etype, PyExc_Exception)) {
            InvalidBSON = _error("InvalidBSON");
            if (InvalidBSON) {
                Py_DECREF(etype);
                etype = InvalidBSON;

                if (evalue) {
                    PyObject *msg = PyObject_Str(evalue);
                    Py_DECREF(evalue);
                    evalue = msg;
                }
                PyErr_NormalizeException(&etype, &evalue, &etrace);
            }
        }
        PyErr_Restore(etype, evalue, etrace);
        return -1;
    }
    position += (unsigned)name_length + 1;
    *value = get_value(self, *name, string, &position, type,
                       max - position, options, raw_array);
    if (!*value) {
        Py_DECREF(*name);
        return -1;
    }
    return position;
}

static PyObject* _cbson_element_to_dict(PyObject* self, PyObject* args) {
    /* TODO: Support buffer protocol */
    char* string;
    PyObject* bson;
    PyObject* options_obj;
    codec_options_t options;
    unsigned position;
    unsigned max;
    int new_position;
    int raw_array = 0;
    PyObject* name;
    PyObject* value;
    PyObject* result_tuple;

    if (!(PyArg_ParseTuple(args, "OIIOp", &bson, &position, &max,
                          &options_obj, &raw_array) &&
            convert_codec_options(self, options_obj, &options))) {
        return NULL;
    }

    if (!PyBytes_Check(bson)) {
        PyErr_SetString(PyExc_TypeError, "argument to _element_to_dict must be a bytes object");
        return NULL;
    }
    string = PyBytes_AS_STRING(bson);

    new_position = _element_to_dict(self, string, position, max, &options, raw_array, &name, &value);
    if (new_position < 0) {
        return NULL;
    }

    result_tuple = Py_BuildValue("NNi", name, value, new_position);
    if (!result_tuple) {
        Py_DECREF(name);
        Py_DECREF(value);
        return NULL;
    }

    destroy_codec_options(&options);
    return result_tuple;
}

static PyObject* _elements_to_dict(PyObject* self, const char* string,
                                   unsigned max,
                                   const codec_options_t* options) {
    unsigned position = 0;
    PyObject* dict = PyObject_CallObject(options->document_class, NULL);
    if (!dict) {
        return NULL;
    }
    int raw_array = 0;
    while (position < max) {
        PyObject* name = NULL;
        PyObject* value = NULL;
        int new_position;

        new_position = _element_to_dict(
            self, string, position, max, options, raw_array, &name, &value);
        if (new_position < 0) {
            Py_DECREF(dict);
            return NULL;
        } else {
            position = (unsigned)new_position;
        }

        PyObject_SetItem(dict, name, value);
        Py_DECREF(name);
        Py_DECREF(value);
    }
    return dict;
}

static PyObject* elements_to_dict(PyObject* self, const char* string,
                                  unsigned max,
                                  const codec_options_t* options) {
    PyObject* result;
    if (options->is_raw_bson) {
        return PyObject_CallFunction(
            options->document_class, "y#O",
            string, max, options->options_obj);
    }
    if (Py_EnterRecursiveCall(" while decoding a BSON document"))
        return NULL;
    result = _elements_to_dict(self, string + 4, max - 5, options);
    Py_LeaveRecursiveCall();
    return result;
}

static int _get_buffer(PyObject *exporter, Py_buffer *view) {
    if (PyObject_GetBuffer(exporter, view, PyBUF_SIMPLE) == -1) {
        return 0;
    }
    if (!PyBuffer_IsContiguous(view, 'C')) {
        PyErr_SetString(PyExc_ValueError,
                        "must be a contiguous buffer");
        goto fail;
    }
    if (!view->buf || view->len < 0) {
        PyErr_SetString(PyExc_ValueError, "invalid buffer");
        goto fail;
    }
    if (view->itemsize != 1) {
        PyErr_SetString(PyExc_ValueError,
                        "buffer data must be ascii or utf8");
        goto fail;
    }
    return 1;
fail:
    PyBuffer_Release(view);
    return 0;
}

static PyObject* _cbson_bson_to_dict(PyObject* self, PyObject* args) {
    int32_t size;
    Py_ssize_t total_size;
    const char* string;
    PyObject* bson;
    codec_options_t options;
    PyObject* result = NULL;
    PyObject* options_obj;
    Py_buffer view = {0};

    if (! (PyArg_ParseTuple(args, "OO", &bson, &options_obj) &&
            convert_codec_options(self, options_obj, &options))) {
        return result;
    }

    if (!_get_buffer(bson, &view)) {
        destroy_codec_options(&options);
        return result;
    }

    total_size = view.len;

    if (total_size < BSON_MIN_SIZE) {
        PyObject* InvalidBSON = _error("InvalidBSON");
        if (InvalidBSON) {
            PyErr_SetString(InvalidBSON,
                            "not enough data for a BSON document");
            Py_DECREF(InvalidBSON);
        }
        goto done;;
    }

    string = (char*)view.buf;
    memcpy(&size, string, 4);
    size = (int32_t)BSON_UINT32_FROM_LE(size);
    if (size < BSON_MIN_SIZE) {
        PyObject* InvalidBSON = _error("InvalidBSON");
        if (InvalidBSON) {
            PyErr_SetString(InvalidBSON, "invalid message size");
            Py_DECREF(InvalidBSON);
        }
        goto done;
    }

    if (total_size < size || total_size > BSON_MAX_SIZE) {
        PyObject* InvalidBSON = _error("InvalidBSON");
        if (InvalidBSON) {
            PyErr_SetString(InvalidBSON, "objsize too large");
            Py_DECREF(InvalidBSON);
        }
        goto done;
    }

    if (size != total_size || string[size - 1]) {
        PyObject* InvalidBSON = _error("InvalidBSON");
        if (InvalidBSON) {
            PyErr_SetString(InvalidBSON, "bad eoo");
            Py_DECREF(InvalidBSON);
        }
        goto done;
    }

    result = elements_to_dict(self, string, (unsigned)size, &options);
done:
    PyBuffer_Release(&view);
    destroy_codec_options(&options);
    return result;
}

static PyObject* _cbson_decode_all(PyObject* self, PyObject* args) {
    int32_t size;
    Py_ssize_t total_size;
    const char* string;
    PyObject* bson;
    PyObject* dict;
    PyObject* result = NULL;
    codec_options_t options;
    PyObject* options_obj = NULL;
    Py_buffer view = {0};

    if (!(PyArg_ParseTuple(args, "OO", &bson, &options_obj) &&
            convert_codec_options(self, options_obj, &options))) {
        return NULL;
    }

    if (!_get_buffer(bson, &view)) {
        destroy_codec_options(&options);
        return NULL;
    }
    total_size = view.len;
    string = (char*)view.buf;

    if (!(result = PyList_New(0))) {
        goto fail;
    }

    while (total_size > 0) {
        if (total_size < BSON_MIN_SIZE) {
            PyObject* InvalidBSON = _error("InvalidBSON");
            if (InvalidBSON) {
                PyErr_SetString(InvalidBSON,
                                "not enough data for a BSON document");
                Py_DECREF(InvalidBSON);
            }
            Py_DECREF(result);
            goto fail;
        }

        memcpy(&size, string, 4);
        size = (int32_t)BSON_UINT32_FROM_LE(size);
        if (size < BSON_MIN_SIZE) {
            PyObject* InvalidBSON = _error("InvalidBSON");
            if (InvalidBSON) {
                PyErr_SetString(InvalidBSON, "invalid message size");
                Py_DECREF(InvalidBSON);
            }
            Py_DECREF(result);
            goto fail;
        }

        if (total_size < size) {
            PyObject* InvalidBSON = _error("InvalidBSON");
            if (InvalidBSON) {
                PyErr_SetString(InvalidBSON, "objsize too large");
                Py_DECREF(InvalidBSON);
            }
            Py_DECREF(result);
            goto fail;
        }

        if (string[size - 1]) {
            PyObject* InvalidBSON = _error("InvalidBSON");
            if (InvalidBSON) {
                PyErr_SetString(InvalidBSON, "bad eoo");
                Py_DECREF(InvalidBSON);
            }
            Py_DECREF(result);
            goto fail;
        }

        dict = elements_to_dict(self, string, (unsigned)size, &options);
        if (!dict) {
            Py_DECREF(result);
            goto fail;
        }
        if (PyList_Append(result, dict) < 0) {
            Py_DECREF(dict);
            Py_DECREF(result);
            goto fail;
        }
        Py_DECREF(dict);
        string += size;
        total_size -= size;
    }
    goto done;
fail:
    result = NULL;
done:
    PyBuffer_Release(&view);
    destroy_codec_options(&options);
    return result;
}


static PyObject* _cbson_array_of_documents_to_buffer(PyObject* self, PyObject* args) {
    uint32_t size;
    uint32_t value_length;
    uint32_t position = 0;
    buffer_t buffer;
    const char* string;
    PyObject* arr;
    PyObject* result = NULL;
    Py_buffer view = {0};

    if (!PyArg_ParseTuple(args, "O", &arr)) {
        return NULL;
    }

    if (!_get_buffer(arr, &view)) {
        return NULL;
    }

    buffer = pymongo_buffer_new();
    if (!buffer) {
        PyBuffer_Release(&view);
        return NULL;
    }

    string = (char*)view.buf;

    if (view.len < BSON_MIN_SIZE) {
        PyObject* InvalidBSON = _error("InvalidBSON");
        if (InvalidBSON) {
            PyErr_SetString(InvalidBSON,
                            "not enough data for a BSON document");
            Py_DECREF(InvalidBSON);
        }
        goto done;
    }

    memcpy(&size, string, 4);
    size = BSON_UINT32_FROM_LE(size);
    /* save space for length */
    if (pymongo_buffer_save_space(buffer, size) == -1) {
        goto fail;
    }
    pymongo_buffer_update_position(buffer, 0);

    position += 4;
    while (position < size - 1) {
        // Verify the value is an object.
        unsigned char type = (unsigned char)string[position];
        if (type != 3) {
            PyObject* InvalidBSON = _error("InvalidBSON");
            if (InvalidBSON) {
                PyErr_SetString(InvalidBSON, "array element was not an object");
                Py_DECREF(InvalidBSON);
            }
            goto fail;
        }

        // Just skip the keys.
        position = position + strlen(string + position) + 1;

        if (position >= size || (size - position) < BSON_MIN_SIZE) {
            PyObject* InvalidBSON = _error("InvalidBSON");
            if (InvalidBSON) {
                PyErr_SetString(InvalidBSON, "invalid array content");
                Py_DECREF(InvalidBSON);
            }
            goto fail;
         }

        memcpy(&value_length, string + position, 4);
        value_length = BSON_UINT32_FROM_LE(value_length);
        if (value_length < BSON_MIN_SIZE) {
            PyObject* InvalidBSON = _error("InvalidBSON");
            if (InvalidBSON) {
                PyErr_SetString(InvalidBSON, "invalid message size");
                Py_DECREF(InvalidBSON);
            }
            goto fail;
         }

        if (view.len < size) {
            PyObject* InvalidBSON = _error("InvalidBSON");
            if (InvalidBSON) {
                PyErr_SetString(InvalidBSON, "objsize too large");
                Py_DECREF(InvalidBSON);
            }
            goto fail;
        }

        if (string[size - 1]) {
            PyObject* InvalidBSON = _error("InvalidBSON");
            if (InvalidBSON) {
                PyErr_SetString(InvalidBSON, "bad eoo");
                Py_DECREF(InvalidBSON);
            }
            goto fail;
        }

        if (pymongo_buffer_write(buffer, string + position, value_length) == 1) {
            goto fail;
        }
        position += value_length;
    }

    /* objectify buffer */
    result = Py_BuildValue("y#", pymongo_buffer_get_buffer(buffer),
                           (Py_ssize_t)pymongo_buffer_get_position(buffer));
    goto done;
fail:
    result = NULL;
done:
    PyBuffer_Release(&view);
    pymongo_buffer_free(buffer);
    return result;
}


static PyMethodDef _CBSONMethods[] = {
    {"_dict_to_bson", _cbson_dict_to_bson, METH_VARARGS,
     "convert a dictionary to a string containing its BSON representation."},
    {"_bson_to_dict", _cbson_bson_to_dict, METH_VARARGS,
     "convert a BSON string to a SON object."},
    {"_decode_all", _cbson_decode_all, METH_VARARGS,
     "convert binary data to a sequence of documents."},
    {"_element_to_dict", _cbson_element_to_dict, METH_VARARGS,
     "Decode a single key, value pair."},
    {"_array_of_documents_to_buffer", _cbson_array_of_documents_to_buffer, METH_VARARGS, "Convert raw array of documents to a stream of BSON documents"},
    {"_test_long_long_to_str", _test_long_long_to_str, METH_VARARGS, "Test conversion of extreme and common Py_ssize_t values to str."},
    {NULL, NULL, 0, NULL}
};

#define INITERROR return -1;
static int _cbson_traverse(PyObject *m, visitproc visit, void *arg) {
    struct module_state *state = GETSTATE(m);
    if (!state) {
        return 0;
    }
    Py_VISIT(state->Binary);
    Py_VISIT(state->Code);
    Py_VISIT(state->ObjectId);
    Py_VISIT(state->DBRef);
    Py_VISIT(state->Regex);
    Py_VISIT(state->UUID);
    Py_VISIT(state->Timestamp);
    Py_VISIT(state->MinKey);
    Py_VISIT(state->MaxKey);
    Py_VISIT(state->UTC);
    Py_VISIT(state->REType);
    Py_VISIT(state->_type_marker_str);
    Py_VISIT(state->_flags_str);
    Py_VISIT(state->_pattern_str);
    Py_VISIT(state->_encoder_map_str);
    Py_VISIT(state->_decoder_map_str);
    Py_VISIT(state->_fallback_encoder_str);
    Py_VISIT(state->_raw_str);
    Py_VISIT(state->_subtype_str);
    Py_VISIT(state->_binary_str);
    Py_VISIT(state->_scope_str);
    Py_VISIT(state->_inc_str);
    Py_VISIT(state->_time_str);
    Py_VISIT(state->_bid_str);
    Py_VISIT(state->_replace_str);
    Py_VISIT(state->_astimezone_str);
    Py_VISIT(state->_id_str);
    Py_VISIT(state->_dollar_ref_str);
    Py_VISIT(state->_dollar_id_str);
    Py_VISIT(state->_dollar_db_str);
    Py_VISIT(state->_tzinfo_str);
    Py_VISIT(state->_as_doc_str);
    Py_VISIT(state->_utcoffset_str);
    Py_VISIT(state->_from_uuid_str);
    Py_VISIT(state->_as_uuid_str);
    Py_VISIT(state->_from_bid_str);
    return 0;
}

static int _cbson_clear(PyObject *m) {
    struct module_state *state = GETSTATE(m);
    if (!state) {
        return 0;
    }
    Py_CLEAR(state->Binary);
    Py_CLEAR(state->Code);
    Py_CLEAR(state->ObjectId);
    Py_CLEAR(state->DBRef);
    Py_CLEAR(state->Regex);
    Py_CLEAR(state->UUID);
    Py_CLEAR(state->Timestamp);
    Py_CLEAR(state->MinKey);
    Py_CLEAR(state->MaxKey);
    Py_CLEAR(state->UTC);
    Py_CLEAR(state->REType);
    Py_CLEAR(state->_type_marker_str);
    Py_CLEAR(state->_flags_str);
    Py_CLEAR(state->_pattern_str);
    Py_CLEAR(state->_encoder_map_str);
    Py_CLEAR(state->_decoder_map_str);
    Py_CLEAR(state->_fallback_encoder_str);
    Py_CLEAR(state->_raw_str);
    Py_CLEAR(state->_subtype_str);
    Py_CLEAR(state->_binary_str);
    Py_CLEAR(state->_scope_str);
    Py_CLEAR(state->_inc_str);
    Py_CLEAR(state->_time_str);
    Py_CLEAR(state->_bid_str);
    Py_CLEAR(state->_replace_str);
    Py_CLEAR(state->_astimezone_str);
    Py_CLEAR(state->_id_str);
    Py_CLEAR(state->_dollar_ref_str);
    Py_CLEAR(state->_dollar_id_str);
    Py_CLEAR(state->_dollar_db_str);
    Py_CLEAR(state->_tzinfo_str);
    Py_CLEAR(state->_as_doc_str);
    Py_CLEAR(state->_utcoffset_str);
    Py_CLEAR(state->_from_uuid_str);
    Py_CLEAR(state->_as_uuid_str);
    Py_CLEAR(state->_from_bid_str);
    return 0;
}

/* Multi-phase extension module initialization code.
 * See https://peps.python.org/pep-0489/.
*/
static int
_cbson_exec(PyObject *m)
{
    PyObject *c_api_object;
    static void *_cbson_API[_cbson_API_POINTER_COUNT];

    PyDateTime_IMPORT;
    if (PyDateTimeAPI == NULL) {
        INITERROR;
    }

    /* Export C API */
    _cbson_API[_cbson_buffer_write_bytes_INDEX] = (void *) buffer_write_bytes;
    _cbson_API[_cbson_write_dict_INDEX] = (void *) write_dict;
    _cbson_API[_cbson_write_pair_INDEX] = (void *) write_pair;
    _cbson_API[_cbson_decode_and_write_pair_INDEX] = (void *) decode_and_write_pair;
    _cbson_API[_cbson_convert_codec_options_INDEX] = (void *) convert_codec_options;
    _cbson_API[_cbson_destroy_codec_options_INDEX] = (void *) destroy_codec_options;
    _cbson_API[_cbson_buffer_write_double_INDEX] = (void *) buffer_write_double;
    _cbson_API[_cbson_buffer_write_int32_INDEX] = (void *) buffer_write_int32;
    _cbson_API[_cbson_buffer_write_int64_INDEX] = (void *) buffer_write_int64;
    _cbson_API[_cbson_buffer_write_int32_at_position_INDEX] =
        (void *) buffer_write_int32_at_position;
    _cbson_API[_cbson_downcast_and_check_INDEX] = (void *) _downcast_and_check;

    c_api_object = PyCapsule_New((void *) _cbson_API, "_cbson._C_API", NULL);
    if (c_api_object == NULL)
        INITERROR;

    /* Import several python objects */
    if (_load_python_objects(m)) {
        Py_DECREF(c_api_object);
        Py_DECREF(m);
        INITERROR;
    }

    if (PyModule_AddObject(m, "_C_API", c_api_object) < 0) {
        Py_DECREF(c_api_object);
        Py_DECREF(m);
        INITERROR;
    }

    return 0;
}

static PyModuleDef_Slot _cbson_slots[] = {
    {Py_mod_exec, _cbson_exec},
#if defined(Py_MOD_MULTIPLE_INTERPRETERS_SUPPORTED)
    {Py_mod_multiple_interpreters, Py_MOD_MULTIPLE_INTERPRETERS_SUPPORTED},
#endif
    {0, NULL},
};


static struct PyModuleDef moduledef = {
    PyModuleDef_HEAD_INIT,
    "_cbson",
    NULL,
    sizeof(struct module_state),
    _CBSONMethods,
    _cbson_slots,
    _cbson_traverse,
    _cbson_clear,
    NULL
};

PyMODINIT_FUNC
PyInit__cbson(void)
{
    return PyModuleDef_Init(&moduledef);
}
