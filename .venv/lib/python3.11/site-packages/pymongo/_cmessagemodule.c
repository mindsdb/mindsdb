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
 * needed by the message module. If possible, these implementations
 * should be used to speed up message creation.
 */

#define PY_SSIZE_T_CLEAN
#include "Python.h"

#include "_cbsonmodule.h"
#include "buffer.h"

struct module_state {
    PyObject* _cbson;
    PyObject* _max_bson_size_str;
    PyObject* _max_message_size_str;
    PyObject* _max_write_batch_size_str;
    PyObject* _max_split_size_str;
};

/* See comments about module initialization in _cbsonmodule.c */
#define GETSTATE(m) ((struct module_state*)PyModule_GetState(m))

#define DOC_TOO_LARGE_FMT "BSON document too large (%d bytes)" \
                          " - the connected server supports" \
                          " BSON document sizes up to %ld bytes."

/* Get an error class from the pymongo.errors module.
 *
 * Returns a new ref */
static PyObject* _error(char* name) {
    PyObject* error;
    PyObject* errors = PyImport_ImportModule("pymongo.errors");
    if (!errors) {
        return NULL;
    }
    error = PyObject_GetAttrString(errors, name);
    Py_DECREF(errors);
    return error;
}

/* The same as buffer_write_bytes except that it also validates
 * "size" will fit in an int.
 * Returns 0 on failure */
static int buffer_write_bytes_ssize_t(buffer_t buffer, const char* data, Py_ssize_t size) {
    int downsize = _downcast_and_check(size, 0);
    if (size == -1) {
        return 0;
    }
    return buffer_write_bytes(buffer, data, downsize);
}

static PyObject* _cbson_query_message(PyObject* self, PyObject* args) {
    /* NOTE just using a random number as the request_id */
    int request_id = rand();
    unsigned int flags;
    char* collection_name = NULL;
    Py_ssize_t collection_name_length;
    int begin, cur_size, max_size = 0;
    int num_to_skip;
    int num_to_return;
    PyObject* query;
    PyObject* field_selector;
    PyObject* options_obj;
    codec_options_t options;
    buffer_t buffer = NULL;
    int length_location, message_length;
    PyObject* result = NULL;
    struct module_state *state = GETSTATE(self);
    if (!state) {
        return NULL;
    }

    if (!(PyArg_ParseTuple(args, "Iet#iiOOO",
                          &flags,
                          "utf-8",
                          &collection_name,
                          &collection_name_length,
                          &num_to_skip, &num_to_return,
                          &query, &field_selector,
                          &options_obj) &&
            convert_codec_options(state->_cbson, options_obj, &options))) {
        return NULL;
    }
    buffer = pymongo_buffer_new();
    if (!buffer) {
        goto fail;
    }

    // save space for message length
    length_location = pymongo_buffer_save_space(buffer, 4);
    if (length_location == -1) {
        goto fail;
    }

    if (!buffer_write_int32(buffer, (int32_t)request_id) ||
        !buffer_write_bytes(buffer, "\x00\x00\x00\x00\xd4\x07\x00\x00", 8) ||
        !buffer_write_int32(buffer, (int32_t)flags) ||
        !buffer_write_bytes_ssize_t(buffer, collection_name,
                                    collection_name_length + 1) ||
        !buffer_write_int32(buffer, (int32_t)num_to_skip) ||
        !buffer_write_int32(buffer, (int32_t)num_to_return)) {
        goto fail;
    }

    begin = pymongo_buffer_get_position(buffer);
    if (!write_dict(state->_cbson, buffer, query, 0, &options, 1)) {
        goto fail;
    }

    max_size = pymongo_buffer_get_position(buffer) - begin;

    if (field_selector != Py_None) {
        begin = pymongo_buffer_get_position(buffer);
        if (!write_dict(state->_cbson, buffer, field_selector, 0,
                        &options, 1)) {
            goto fail;
        }
        cur_size = pymongo_buffer_get_position(buffer) - begin;
        max_size = (cur_size > max_size) ? cur_size : max_size;
    }

    message_length = pymongo_buffer_get_position(buffer) - length_location;
    buffer_write_int32_at_position(
        buffer, length_location, (int32_t)message_length);

    /* objectify buffer */
    result = Py_BuildValue("iy#i", request_id,
                           pymongo_buffer_get_buffer(buffer),
                           (Py_ssize_t)pymongo_buffer_get_position(buffer),
                           max_size);
fail:
    PyMem_Free(collection_name);
    destroy_codec_options(&options);
    if (buffer) {
        pymongo_buffer_free(buffer);
    }
    return result;
}

static PyObject* _cbson_get_more_message(PyObject* self, PyObject* args) {
    /* NOTE just using a random number as the request_id */
    int request_id = rand();
    char* collection_name = NULL;
    Py_ssize_t collection_name_length;
    int num_to_return;
    long long cursor_id;
    buffer_t buffer = NULL;
    int length_location, message_length;
    PyObject* result = NULL;

    if (!PyArg_ParseTuple(args, "et#iL",
                          "utf-8",
                          &collection_name,
                          &collection_name_length,
                          &num_to_return,
                          &cursor_id)) {
        return NULL;
    }
    buffer = pymongo_buffer_new();
    if (!buffer) {
        goto fail;
    }

    // save space for message length
    length_location = pymongo_buffer_save_space(buffer, 4);
    if (length_location == -1) {
        goto fail;
    }
    if (!buffer_write_int32(buffer, (int32_t)request_id) ||
        !buffer_write_bytes(buffer,
                            "\x00\x00\x00\x00"
                            "\xd5\x07\x00\x00"
                            "\x00\x00\x00\x00", 12) ||
        !buffer_write_bytes_ssize_t(buffer,
                                    collection_name,
                                    collection_name_length + 1) ||
        !buffer_write_int32(buffer, (int32_t)num_to_return) ||
        !buffer_write_int64(buffer, (int64_t)cursor_id)) {
        goto fail;
    }

    message_length = pymongo_buffer_get_position(buffer) - length_location;
    buffer_write_int32_at_position(
        buffer, length_location, (int32_t)message_length);

    /* objectify buffer */
    result = Py_BuildValue("iy#", request_id,
                           pymongo_buffer_get_buffer(buffer),
                           (Py_ssize_t)pymongo_buffer_get_position(buffer));
fail:
    PyMem_Free(collection_name);
    if (buffer) {
        pymongo_buffer_free(buffer);
    }
    return result;
}

/*
 * NOTE this method handles multiple documents in a type one payload but
 * it does not perform batch splitting and the total message size is
 * only checked *after* generating the entire message.
 */
static PyObject* _cbson_op_msg(PyObject* self, PyObject* args) {
    /* NOTE just using a random number as the request_id */
    int request_id = rand();
    unsigned int flags;
    PyObject* command;
    char* identifier = NULL;
    Py_ssize_t identifier_length = 0;
    PyObject* docs;
    PyObject* doc;
    PyObject* options_obj;
    codec_options_t options;
    buffer_t buffer = NULL;
    int length_location, message_length;
    int total_size = 0;
    int max_doc_size = 0;
    PyObject* result = NULL;
    PyObject* iterator = NULL;
    struct module_state *state = GETSTATE(self);
    if (!state) {
        return NULL;
    }

    /*flags, command, identifier, docs, opts*/
    if (!(PyArg_ParseTuple(args, "IOet#OO",
                          &flags,
                          &command,
                          "utf-8",
                          &identifier,
                          &identifier_length,
                          &docs,
                          &options_obj) &&
            convert_codec_options(state->_cbson, options_obj, &options))) {
        return NULL;
    }
    buffer = pymongo_buffer_new();
    if (!buffer) {
        goto fail;
    }

    // save space for message length
    length_location = pymongo_buffer_save_space(buffer, 4);
    if (length_location == -1) {
        goto fail;
    }
    if (!buffer_write_int32(buffer, (int32_t)request_id) ||
        !buffer_write_bytes(buffer,
                            "\x00\x00\x00\x00" /* responseTo */
                            "\xdd\x07\x00\x00" /* 2013 */, 8)) {
        goto fail;
    }

    if (!buffer_write_int32(buffer, (int32_t)flags) ||
        !buffer_write_bytes(buffer, "\x00", 1) /* Payload type 0 */) {
        goto fail;
    }
    total_size = write_dict(state->_cbson, buffer, command, 0,
                          &options, 1);
    if (!total_size) {
        goto fail;
    }

    if (identifier_length) {
        int payload_one_length_location, payload_length;
        /* Payload type 1 */
        if (!buffer_write_bytes(buffer, "\x01", 1)) {
            goto fail;
        }
        /* save space for payload 0 length */
        payload_one_length_location = pymongo_buffer_save_space(buffer, 4);
        /* C string identifier */
        if (!buffer_write_bytes_ssize_t(buffer, identifier, identifier_length + 1)) {
            goto fail;
        }
        iterator = PyObject_GetIter(docs);
        if (iterator == NULL) {
            goto fail;
        }
        while ((doc = PyIter_Next(iterator)) != NULL) {
            int encoded_doc_size = write_dict(
                      state->_cbson, buffer, doc, 0, &options, 1);
            if (!encoded_doc_size) {
                Py_CLEAR(doc);
                goto fail;
            }
            if (encoded_doc_size > max_doc_size) {
                max_doc_size = encoded_doc_size;
            }
            Py_CLEAR(doc);
        }

        payload_length = pymongo_buffer_get_position(buffer) - payload_one_length_location;
        buffer_write_int32_at_position(
            buffer, payload_one_length_location, (int32_t)payload_length);
        total_size += payload_length;
    }

    message_length = pymongo_buffer_get_position(buffer) - length_location;
    buffer_write_int32_at_position(
        buffer, length_location, (int32_t)message_length);

    /* objectify buffer */
    result = Py_BuildValue("iy#ii", request_id,
                           pymongo_buffer_get_buffer(buffer),
                           (Py_ssize_t)pymongo_buffer_get_position(buffer),
                           total_size,
                           max_doc_size);
fail:
    Py_XDECREF(iterator);
    if (buffer) {
        pymongo_buffer_free(buffer);
    }
    PyMem_Free(identifier);
    destroy_codec_options(&options);
    return result;
}


static void
_set_document_too_large(int size, long max) {
    PyObject* DocumentTooLarge = _error("DocumentTooLarge");
    if (DocumentTooLarge) {
        PyObject* error = PyUnicode_FromFormat(DOC_TOO_LARGE_FMT, size, max);
        if (error) {
            PyErr_SetObject(DocumentTooLarge, error);
            Py_DECREF(error);
        }
        Py_DECREF(DocumentTooLarge);
    }
}

#define _INSERT 0
#define _UPDATE 1
#define _DELETE 2

/* OP_MSG ----------------------------------------------- */

static int
_batched_op_msg(
        unsigned char op, unsigned char ack,
        PyObject* command, PyObject* docs, PyObject* ctx,
        PyObject* to_publish, codec_options_t options,
        buffer_t buffer, struct module_state *state) {

    long max_bson_size;
    long max_write_batch_size;
    long max_message_size;
    int idx = 0;
    int size_location;
    int position;
    int length;
    PyObject* max_bson_size_obj = NULL;
    PyObject* max_write_batch_size_obj = NULL;
    PyObject* max_message_size_obj = NULL;
    PyObject* doc = NULL;
    PyObject* iterator = NULL;
    char* flags = ack ? "\x00\x00\x00\x00" : "\x02\x00\x00\x00";

    max_bson_size_obj = PyObject_GetAttr(ctx, state->_max_bson_size_str);
    max_bson_size = PyLong_AsLong(max_bson_size_obj);
    Py_XDECREF(max_bson_size_obj);
    if (max_bson_size == -1) {
        return 0;
    }

    max_write_batch_size_obj = PyObject_GetAttr(ctx, state->_max_write_batch_size_str);
    max_write_batch_size = PyLong_AsLong(max_write_batch_size_obj);
    Py_XDECREF(max_write_batch_size_obj);
    if (max_write_batch_size == -1) {
        return 0;
    }

    max_message_size_obj = PyObject_GetAttr(ctx, state->_max_message_size_str);
    max_message_size = PyLong_AsLong(max_message_size_obj);
    Py_XDECREF(max_message_size_obj);
    if (max_message_size == -1) {
        return 0;
    }

    if (!buffer_write_bytes(buffer, flags, 4)) {
        return 0;
    }
    /* Type 0 Section */
    if (!buffer_write_bytes(buffer, "\x00", 1)) {
        return 0;
    }
    if (!write_dict(state->_cbson, buffer, command, 0,
                    &options, 0)) {
        return 0;
    }

    /* Type 1 Section */
    if (!buffer_write_bytes(buffer, "\x01", 1)) {
        return 0;
    }
    /* Save space for size */
    size_location = pymongo_buffer_save_space(buffer, 4);
    if (size_location == -1) {
        return 0;
    }

    switch (op) {
    case _INSERT:
        {
            if (!buffer_write_bytes(buffer, "documents\x00", 10))
                goto fail;
            break;
        }
    case _UPDATE:
        {
            if (!buffer_write_bytes(buffer, "updates\x00", 8))
                goto fail;
            break;
        }
    case _DELETE:
        {
            if (!buffer_write_bytes(buffer, "deletes\x00", 8))
                goto fail;
            break;
        }
    default:
        {
            PyObject* InvalidOperation = _error("InvalidOperation");
            if (InvalidOperation) {
                PyErr_SetString(InvalidOperation, "Unknown command");
                Py_DECREF(InvalidOperation);
            }
            return 0;
        }
    }

    iterator = PyObject_GetIter(docs);
    if (iterator == NULL) {
        PyObject* InvalidOperation = _error("InvalidOperation");
        if (InvalidOperation) {
            PyErr_SetString(InvalidOperation, "input is not iterable");
            Py_DECREF(InvalidOperation);
        }
        return 0;
    }
    while ((doc = PyIter_Next(iterator)) != NULL) {
        int cur_doc_begin = pymongo_buffer_get_position(buffer);
        int cur_size;
        int doc_too_large = 0;
        int unacked_doc_too_large = 0;
        if (!write_dict(state->_cbson, buffer, doc, 0, &options, 1)) {
            goto fail;
        }
        cur_size = pymongo_buffer_get_position(buffer) - cur_doc_begin;

        /* Does the first document exceed max_message_size? */
        doc_too_large = (idx == 0 && (pymongo_buffer_get_position(buffer) > max_message_size));
        /* When OP_MSG is used unacknowledged we have to check
         * document size client side or applications won't be notified.
         * Otherwise we let the server deal with documents that are too large
         * since ordered=False causes those documents to be skipped instead of
         * halting the bulk write operation.
         * */
        unacked_doc_too_large = (!ack && cur_size > max_bson_size);
        if (doc_too_large || unacked_doc_too_large) {
            if (op == _INSERT) {
                _set_document_too_large(cur_size, max_bson_size);
            } else {
                PyObject* DocumentTooLarge = _error("DocumentTooLarge");
                if (DocumentTooLarge) {
                    /*
                     * There's nothing intelligent we can say
                     * about size for update and delete.
                     */
                    PyErr_Format(
                        DocumentTooLarge,
                        "%s command document too large",
                        (op == _UPDATE) ? "update": "delete");
                    Py_DECREF(DocumentTooLarge);
                }
            }
            goto fail;
        }
        /* We have enough data, return this batch. */
        if (pymongo_buffer_get_position(buffer) > max_message_size) {
            /*
             * Roll the existing buffer back to the beginning
             * of the last document encoded.
             */
            pymongo_buffer_update_position(buffer, cur_doc_begin);
            Py_CLEAR(doc);
            break;
        }
        if (PyList_Append(to_publish, doc) < 0) {
            goto fail;
        }
        Py_CLEAR(doc);
        idx += 1;
        /* We have enough documents, return this batch. */
        if (idx == max_write_batch_size) {
            break;
        }
    }
    Py_CLEAR(iterator);

    if (PyErr_Occurred()) {
        goto fail;
    }

    position = pymongo_buffer_get_position(buffer);
    length = position - size_location;
    buffer_write_int32_at_position(buffer, size_location, (int32_t)length);
    return 1;

fail:
    Py_XDECREF(doc);
    Py_XDECREF(iterator);
    return 0;
}

static PyObject*
_cbson_encode_batched_op_msg(PyObject* self, PyObject* args) {
    unsigned char op;
    unsigned char ack;
    PyObject* command;
    PyObject* docs;
    PyObject* ctx = NULL;
    PyObject* to_publish = NULL;
    PyObject* result = NULL;
    PyObject* options_obj;
    codec_options_t options;
    buffer_t buffer;
    struct module_state *state = GETSTATE(self);
    if (!state) {
        return NULL;
    }

    if (!(PyArg_ParseTuple(args, "bOObOO",
                          &op, &command, &docs, &ack,
                          &options_obj, &ctx) &&
            convert_codec_options(state->_cbson, options_obj, &options))) {
        return NULL;
    }
    if (!(buffer = pymongo_buffer_new())) {
        destroy_codec_options(&options);
        return NULL;
    }
    if (!(to_publish = PyList_New(0))) {
        goto fail;
    }

    if (!_batched_op_msg(
            op,
            ack,
            command,
            docs,
            ctx,
            to_publish,
            options,
            buffer,
            state)) {
        goto fail;
    }

    result = Py_BuildValue("y#O",
                           pymongo_buffer_get_buffer(buffer),
                           (Py_ssize_t)pymongo_buffer_get_position(buffer),
                           to_publish);
fail:
    destroy_codec_options(&options);
    pymongo_buffer_free(buffer);
    Py_XDECREF(to_publish);
    return result;
}

static PyObject*
_cbson_batched_op_msg(PyObject* self, PyObject* args) {
    unsigned char op;
    unsigned char ack;
    int request_id;
    int position;
    PyObject* command;
    PyObject* docs;
    PyObject* ctx = NULL;
    PyObject* to_publish = NULL;
    PyObject* result = NULL;
    PyObject* options_obj;
    codec_options_t options;
    buffer_t buffer;
    struct module_state *state = GETSTATE(self);
    if (!state) {
        return NULL;
    }

    if (!(PyArg_ParseTuple(args, "bOObOO",
                          &op, &command, &docs, &ack,
                          &options_obj, &ctx) &&
            convert_codec_options(state->_cbson, options_obj, &options))) {
        return NULL;
    }
    if (!(buffer = pymongo_buffer_new())) {
        destroy_codec_options(&options);
        return NULL;
    }
    /* Save space for message length and request id */
    if ((pymongo_buffer_save_space(buffer, 8)) == -1) {
        goto fail;
    }
    if (!buffer_write_bytes(buffer,
                            "\x00\x00\x00\x00"  /* responseTo */
                            "\xdd\x07\x00\x00", /* opcode */
                            8)) {
        goto fail;
    }
    if (!(to_publish = PyList_New(0))) {
        goto fail;
    }

    if (!_batched_op_msg(
            op,
            ack,
            command,
            docs,
            ctx,
            to_publish,
            options,
            buffer,
            state)) {
        goto fail;
    }

    request_id = rand();
    position = pymongo_buffer_get_position(buffer);
    buffer_write_int32_at_position(buffer, 0, (int32_t)position);
    buffer_write_int32_at_position(buffer, 4, (int32_t)request_id);
    result = Py_BuildValue("iy#O", request_id,
                           pymongo_buffer_get_buffer(buffer),
                           (Py_ssize_t)pymongo_buffer_get_position(buffer),
                           to_publish);
fail:
    destroy_codec_options(&options);
    pymongo_buffer_free(buffer);
    Py_XDECREF(to_publish);
    return result;
}

/* End OP_MSG -------------------------------------------- */

static int
_batched_write_command(
        char* ns, Py_ssize_t ns_len, unsigned char op,
        PyObject* command, PyObject* docs, PyObject* ctx,
        PyObject* to_publish, codec_options_t options,
        buffer_t buffer, struct module_state *state) {

    long max_bson_size;
    long max_cmd_size;
    long max_write_batch_size;
    long max_split_size;
    int idx = 0;
    int cmd_len_loc;
    int lst_len_loc;
    int position;
    int length;
    PyObject* max_bson_size_obj = NULL;
    PyObject* max_write_batch_size_obj = NULL;
    PyObject* max_split_size_obj = NULL;
    PyObject* doc = NULL;
    PyObject* iterator = NULL;

    max_bson_size_obj = PyObject_GetAttr(ctx, state->_max_bson_size_str);
    max_bson_size = PyLong_AsLong(max_bson_size_obj);
    Py_XDECREF(max_bson_size_obj);
    if (max_bson_size == -1) {
        return 0;
    }
    /*
     * Max BSON object size + 16k - 2 bytes for ending NUL bytes
     * XXX: This should come from the server - SERVER-10643
     */
    max_cmd_size = max_bson_size + 16382;

    max_write_batch_size_obj = PyObject_GetAttr(ctx, state->_max_write_batch_size_str);
    max_write_batch_size = PyLong_AsLong(max_write_batch_size_obj);
    Py_XDECREF(max_write_batch_size_obj);
    if (max_write_batch_size == -1) {
        return 0;
    }

    // max_split_size is the size at which to perform a batch split.
    // Normally this this value is equal to max_bson_size (16MiB). However,
    // when auto encryption is enabled max_split_size is reduced to 2MiB.
    max_split_size_obj = PyObject_GetAttr(ctx, state->_max_split_size_str);
    max_split_size = PyLong_AsLong(max_split_size_obj);
    Py_XDECREF(max_split_size_obj);
    if (max_split_size == -1) {
        return 0;
    }

    if (!buffer_write_bytes(buffer,
                            "\x00\x00\x00\x00", /* flags */
                            4) ||
        !buffer_write_bytes_ssize_t(buffer, ns, ns_len + 1) || /* namespace */
        !buffer_write_bytes(buffer,
                            "\x00\x00\x00\x00"  /* skip */
                            "\xFF\xFF\xFF\xFF", /* limit (-1) */
                             8)) {
        return 0;
    }

    /* Position of command document length */
    cmd_len_loc = pymongo_buffer_get_position(buffer);
    if (!write_dict(state->_cbson, buffer, command, 0,
                    &options, 0)) {
        return 0;
    }

    /* Write type byte for array */
    *(pymongo_buffer_get_buffer(buffer) + (pymongo_buffer_get_position(buffer) - 1)) = 0x4;

    switch (op) {
    case _INSERT:
        {
            if (!buffer_write_bytes(buffer, "documents\x00", 10))
                goto fail;
            break;
        }
    case _UPDATE:
        {
            if (!buffer_write_bytes(buffer, "updates\x00", 8))
                goto fail;
            break;
        }
    case _DELETE:
        {
            if (!buffer_write_bytes(buffer, "deletes\x00", 8))
                goto fail;
            break;
        }
    default:
        {
            PyObject* InvalidOperation = _error("InvalidOperation");
            if (InvalidOperation) {
                PyErr_SetString(InvalidOperation, "Unknown command");
                Py_DECREF(InvalidOperation);
            }
            return 0;
        }
    }

    /* Save space for list document */
    lst_len_loc = pymongo_buffer_save_space(buffer, 4);
    if (lst_len_loc == -1) {
        return 0;
    }

    iterator = PyObject_GetIter(docs);
    if (iterator == NULL) {
        PyObject* InvalidOperation = _error("InvalidOperation");
        if (InvalidOperation) {
            PyErr_SetString(InvalidOperation, "input is not iterable");
            Py_DECREF(InvalidOperation);
        }
        return 0;
    }
    while ((doc = PyIter_Next(iterator)) != NULL) {
        int sub_doc_begin = pymongo_buffer_get_position(buffer);
        int cur_doc_begin;
        int cur_size;
        int enough_data = 0;
        char key[BUF_SIZE];
        int res = LL2STR(key, (long long)idx);
        if (res == -1) {
            return 0;
        }
        if (!buffer_write_bytes(buffer, "\x03", 1) ||
            !buffer_write_bytes(buffer, key, (int)strlen(key) + 1)) {
            goto fail;
        }
        cur_doc_begin = pymongo_buffer_get_position(buffer);
        if (!write_dict(state->_cbson, buffer, doc, 0, &options, 1)) {
            goto fail;
        }

        /* We have enough data, return this batch.
         * max_cmd_size accounts for the two trailing null bytes.
         */
        cur_size = pymongo_buffer_get_position(buffer) - cur_doc_begin;
        /* This single document is too large for the command. */
        if (cur_size > max_cmd_size) {
            if (op == _INSERT) {
                _set_document_too_large(cur_size, max_bson_size);
            } else {
                PyObject* DocumentTooLarge = _error("DocumentTooLarge");
                if (DocumentTooLarge) {
                    /*
                     * There's nothing intelligent we can say
                     * about size for update and delete.
                     */
                    PyErr_Format(
                        DocumentTooLarge,
                        "%s command document too large",
                        (op == _UPDATE) ? "update": "delete");
                    Py_DECREF(DocumentTooLarge);
                }
            }
            goto fail;
        }
        enough_data = (idx >= 1 &&
                       (pymongo_buffer_get_position(buffer) > max_split_size));
        if (enough_data) {
            /*
             * Roll the existing buffer back to the beginning
             * of the last document encoded.
             */
            pymongo_buffer_update_position(buffer, sub_doc_begin);
            Py_CLEAR(doc);
            break;
        }
        if (PyList_Append(to_publish, doc) < 0) {
            goto fail;
        }
        Py_CLEAR(doc);
        idx += 1;
        /* We have enough documents, return this batch. */
        if (idx == max_write_batch_size) {
            break;
        }
    }
    Py_CLEAR(iterator);

    if (PyErr_Occurred()) {
        goto fail;
    }

    if (!buffer_write_bytes(buffer, "\x00\x00", 2)) {
        goto fail;
    }

    position = pymongo_buffer_get_position(buffer);
    length = position - lst_len_loc - 1;
    buffer_write_int32_at_position(buffer, lst_len_loc, (int32_t)length);
    length = position - cmd_len_loc;
    buffer_write_int32_at_position(buffer, cmd_len_loc, (int32_t)length);
    return 1;

fail:
    Py_XDECREF(doc);
    Py_XDECREF(iterator);
    return 0;
}

static PyObject*
_cbson_encode_batched_write_command(PyObject* self, PyObject* args) {
    char *ns = NULL;
    unsigned char op;
    Py_ssize_t ns_len;
    PyObject* command;
    PyObject* docs;
    PyObject* ctx = NULL;
    PyObject* to_publish = NULL;
    PyObject* result = NULL;
    PyObject* options_obj;
    codec_options_t options;
    buffer_t buffer;
    struct module_state *state = GETSTATE(self);
    if (!state) {
        return NULL;
    }

    if (!(PyArg_ParseTuple(args, "et#bOOOO", "utf-8",
                          &ns, &ns_len, &op, &command, &docs,
                          &options_obj, &ctx) &&
            convert_codec_options(state->_cbson, options_obj, &options))) {
        return NULL;
    }
    if (!(buffer = pymongo_buffer_new())) {
        PyMem_Free(ns);
        destroy_codec_options(&options);
        return NULL;
    }
    if (!(to_publish = PyList_New(0))) {
        goto fail;
    }

    if (!_batched_write_command(
            ns,
            ns_len,
            op,
            command,
            docs,
            ctx,
            to_publish,
            options,
            buffer,
            state)) {
        goto fail;
    }

    result = Py_BuildValue("y#O",
                           pymongo_buffer_get_buffer(buffer),
                           (Py_ssize_t)pymongo_buffer_get_position(buffer),
                           to_publish);
fail:
    PyMem_Free(ns);
    destroy_codec_options(&options);
    pymongo_buffer_free(buffer);
    Py_XDECREF(to_publish);
    return result;
}

static PyMethodDef _CMessageMethods[] = {
    {"_query_message", _cbson_query_message, METH_VARARGS,
     "create a query message to be sent to MongoDB"},
    {"_get_more_message", _cbson_get_more_message, METH_VARARGS,
     "create a get more message to be sent to MongoDB"},
    {"_op_msg", _cbson_op_msg, METH_VARARGS,
     "create an OP_MSG message to be sent to MongoDB"},
    {"_encode_batched_write_command", _cbson_encode_batched_write_command, METH_VARARGS,
     "Encode the next batched insert, update, or delete command"},
    {"_batched_op_msg", _cbson_batched_op_msg, METH_VARARGS,
     "Create the next batched insert, update, or delete using OP_MSG"},
    {"_encode_batched_op_msg", _cbson_encode_batched_op_msg, METH_VARARGS,
     "Encode the next batched insert, update, or delete using OP_MSG"},
    {NULL, NULL, 0, NULL}
};

#define INITERROR return -1;
static int _cmessage_traverse(PyObject *m, visitproc visit, void *arg) {
    struct module_state *state = GETSTATE(m);
    if (!state) {
        return 0;
    }
    Py_VISIT(state->_cbson);
    Py_VISIT(state->_max_bson_size_str);
    Py_VISIT(state->_max_message_size_str);
    Py_VISIT(state->_max_split_size_str);
    Py_VISIT(state->_max_write_batch_size_str);
    return 0;
}

static int _cmessage_clear(PyObject *m) {
    struct module_state *state = GETSTATE(m);
    if (!state) {
        return 0;
    }
    Py_CLEAR(state->_cbson);
    Py_CLEAR(state->_max_bson_size_str);
    Py_CLEAR(state->_max_message_size_str);
    Py_CLEAR(state->_max_split_size_str);
    Py_CLEAR(state->_max_write_batch_size_str);
    return 0;
}

/* Multi-phase extension module initialization code.
 * See https://peps.python.org/pep-0489/.
*/
static int
_cmessage_exec(PyObject *m)
{
    PyObject *_cbson = NULL;
    PyObject *c_api_object = NULL;
    struct module_state* state = NULL;

    /* Store a reference to the _cbson module since it's needed to call some
     * of its functions
     */
    _cbson = PyImport_ImportModule("bson._cbson");
    if (_cbson == NULL) {
        goto fail;
    }

    /* Import C API of _cbson
     * The header file accesses _cbson_API to call the functions
     */
    c_api_object = PyObject_GetAttrString(_cbson, "_C_API");
    if (c_api_object == NULL) {
        goto fail;
    }
    _cbson_API = (void **)PyCapsule_GetPointer(c_api_object, "_cbson._C_API");
    if (_cbson_API == NULL) {
        goto fail;
    }

    state = GETSTATE(m);
    if (state == NULL) {
        goto fail;
    }
    state->_cbson = _cbson;
    if (!((state->_max_bson_size_str = PyUnicode_FromString("max_bson_size")) &&
        (state->_max_message_size_str = PyUnicode_FromString("max_message_size")) &&
        (state->_max_write_batch_size_str = PyUnicode_FromString("max_write_batch_size")) &&
        (state->_max_split_size_str = PyUnicode_FromString("max_split_size")))) {
            goto fail;
        }

    Py_DECREF(c_api_object);
    return 0;

fail:
    Py_XDECREF(m);
    Py_XDECREF(c_api_object);
    Py_XDECREF(_cbson);
    INITERROR;
}


static PyModuleDef_Slot _cmessage_slots[] = {
    {Py_mod_exec, _cmessage_exec},
#ifdef Py_MOD_MULTIPLE_INTERPRETERS_SUPPORTED
    {Py_mod_multiple_interpreters, Py_MOD_MULTIPLE_INTERPRETERS_SUPPORTED},
#endif
    {0, NULL},
};


static struct PyModuleDef moduledef = {
        PyModuleDef_HEAD_INIT,
        "_cmessage",
        NULL,
        sizeof(struct module_state),
        _CMessageMethods,
        _cmessage_slots,
        _cmessage_traverse,
        _cmessage_clear,
        NULL
};

PyMODINIT_FUNC
PyInit__cmessage(void)
{
    return PyModuleDef_Init(&moduledef);
}
