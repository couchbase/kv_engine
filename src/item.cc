/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2010 Couchbase, Inc
 *
 *   Licensed under the Apache License, Version 2.0 (the "License");
 *   you may not use this file except in compliance with the License.
 *   You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 *   Unless required by applicable law or agreed to in writing, software
 *   distributed under the License is distributed on an "AS IS" BASIS,
 *   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *   See the License for the specific language governing permissions and
 *   limitations under the License.
 */

#include "config.h"

#include "compress.h"
#include "item.h"
#include "cJSON.h"

AtomicValue<uint64_t> Item::casCounter(1);
const uint32_t Item::metaDataSize(2*sizeof(uint32_t) + 2*sizeof(uint64_t) + 2);

/**
 * Append another item to this item
 *
 * @param itm the item to append to this one
 * @param maxItemSize maximum item size permitted
 * @return true if success
 */
ENGINE_ERROR_CODE Item::append(const Item &i, size_t maxItemSize) {
    cb_assert(value.get() != NULL);
    cb_assert(i.getValue().get() != NULL);

    switch(value->getDataType()) {
    case PROTOCOL_BINARY_RAW_BYTES:
    case PROTOCOL_BINARY_DATATYPE_JSON:
        {
            switch(i.getDataType()) {
            case PROTOCOL_BINARY_RAW_BYTES:
            case PROTOCOL_BINARY_DATATYPE_JSON:
                // APPEND NEW TO ORIGINAL AS IS
                {
                    size_t newSize = value->vlength() + i.getValue()->vlength();
                    Blob *newData = Blob::New(newSize, value->getExtLen());
                    if (!newData) {
                        return ENGINE_ENOMEM;
                    } else if (newData->length() > maxItemSize) {
                        LOG(EXTENSION_LOG_WARNING,
                                "New Item size is greater than permitted value "
                                "size, aborting operation (Append)");
                        delete newData;
                        return ENGINE_E2BIG;
                    }
                    char *newValue = (char *) newData->getBlob();
                    std::memcpy(newValue, value->getBlob(), value->length());
                    std::memcpy(newValue + value->length(),
                                i.getValue()->getData(),
                                i.getValue()->vlength());
                    value.reset(newData);
                    return ENGINE_SUCCESS;
                }
            case PROTOCOL_BINARY_DATATYPE_COMPRESSED:
            case PROTOCOL_BINARY_DATATYPE_COMPRESSED_JSON:
                // UNCOMPRESS NEW AND APPEND
                {
                    snap_buf output;
                    snap_ret_t ret = doSnappyUncompress(i.getValue()->getData(),
                                                        i.getValue()->vlength(),
                                                        output);
                    if (ret == SNAP_FAILURE) {
                        LOG(EXTENSION_LOG_WARNING, "(Append) Snappy uncompression "
                            "for append value of key: %s failed!",
                            i.getKey().c_str());
                        return ENGINE_FAILED;
                    }
                    size_t newSize = value->vlength() + output.len;
                    Blob *newData = Blob::New(newSize, value->getExtLen());
                    if (!newData) {
                        return ENGINE_ENOMEM;
                    } else if (newData->length() > maxItemSize) {
                        LOG(EXTENSION_LOG_WARNING,
                                "New Item size is greater than permitted value "
                                "size, aborting operation (Append)");
                        delete newData;
                        return ENGINE_E2BIG;
                    }
                    char *newValue = (char *) newData->getBlob();
                    std::memcpy(newValue, value->getBlob(), value->length());
                    std::memcpy(newValue + value->length(),
                                output.buf.get(), output.len);
                    value.reset(newData);
                    return ENGINE_SUCCESS;
                }
            }
        }
    case PROTOCOL_BINARY_DATATYPE_COMPRESSED:
    case PROTOCOL_BINARY_DATATYPE_COMPRESSED_JSON:
        {
            switch(i.getDataType()) {
            case PROTOCOL_BINARY_RAW_BYTES:
            case PROTOCOL_BINARY_DATATYPE_JSON:
                // UNCOMPRESS ORIGINAL AND APPEND NEW, COMPRESS EVERYTHING
                {
                    snap_buf output;
                    snap_ret_t ret = doSnappyUncompress(value->getData(),
                                                        value->vlength(),
                                                        output);
                    if (ret == SNAP_FAILURE) {
                        LOG(EXTENSION_LOG_WARNING, "(Append) Snappy uncompression "
                            "for original value of key: %s failed!",
                            i.getKey().c_str());
                        return ENGINE_FAILED;
                    }
                    size_t len = output.len + i.getValue()->vlength();
                    std::unique_ptr<char[]> buf(new char[len]);
                    if (!buf) {
                        return ENGINE_ENOMEM;
                    }
                    std::memcpy(buf.get(), output.buf.get(), output.len);
                    std::memcpy(buf.get() + output.len, i.getValue()->getData(),
                                i.getValue()->vlength());

                    snap_buf compOutput;
                    ret = doSnappyCompress(buf.get(), len, compOutput);
                    if (ret == SNAP_FAILURE) {
                        LOG(EXTENSION_LOG_WARNING, "(Append) Snappy compression "
                            "for new value of key: %s failed!",
                            i.getKey().c_str());
                        return ENGINE_FAILED;
                    }
                    Blob *newData = Blob::New(compOutput.len,
                                              value->getExtLen());
                    if (!newData) {
                        return ENGINE_ENOMEM;
                    } else if (newData->length() > maxItemSize) {
                        LOG(EXTENSION_LOG_WARNING,
                                "New Item size is greater than permitted value "
                                "size, aborting operation (Append)");
                        delete newData;
                        return ENGINE_E2BIG;
                    }
                    char *newValue = (char *) newData->getBlob();
                    std::memcpy(newValue, value->getBlob(),
                                FLEX_DATA_OFFSET + value->getExtLen());
                    std::memcpy(newValue + FLEX_DATA_OFFSET + value->getExtLen(),
                                compOutput.buf.get(), compOutput.len);
                    value.reset(newData);
                    return ENGINE_SUCCESS;
                }
            case PROTOCOL_BINARY_DATATYPE_COMPRESSED:
            case PROTOCOL_BINARY_DATATYPE_COMPRESSED_JSON:
                // UNCOMPRESS ORIGINAL AND UNCOMPRESS NEW,
                // THEN APPEND AND COMPRESS EVERYTHING
                {
                    snap_buf output1;
                    snap_ret_t ret = doSnappyUncompress(value->getData(),
                                                        value->vlength(),
                                                        output1);
                    if (ret == SNAP_FAILURE) {
                        LOG(EXTENSION_LOG_WARNING, "(Append) Snappy uncompression "
                            "for original value of key: %s failed!",
                            i.getKey().c_str());
                        return ENGINE_FAILED;
                    }
                    snap_buf output2;
                    ret = doSnappyUncompress(i.getValue()->getData(),
                                             i.getValue()->vlength(),
                                             output2);
                    if (ret == SNAP_FAILURE) {
                        LOG(EXTENSION_LOG_WARNING, "(Append) Snappy uncompression "
                            "for append value of key: %s failed!",
                            i.getKey().c_str());
                        return ENGINE_FAILED;
                    }
                    size_t len = output1.len + output2.len;
                    std::unique_ptr<char[]> buf(new char[len]);
                    if (!buf) {
                        return ENGINE_ENOMEM;
                    }
                    std::memcpy(buf.get(), output1.buf.get(), output1.len);
                    std::memcpy(buf.get() + output1.len, output2.buf.get(), output2.len);

                    snap_buf compOutput;
                    ret = doSnappyCompress(buf.get(), len, compOutput);
                    if (ret == SNAP_FAILURE) {
                        LOG(EXTENSION_LOG_WARNING, "(Append) Snappy compression "
                            "for new value of key: %s failed!",
                            i.getKey().c_str());
                        return ENGINE_FAILED;
                    }
                    Blob *newData = Blob::New(compOutput.len, value->getExtLen());
                    if (!newData) {
                        return ENGINE_ENOMEM;
                    } else if (newData->length() > maxItemSize) {
                        LOG(EXTENSION_LOG_WARNING,
                                "New Item size is greater than permitted value "
                                "size, aborting operation (Append)");
                        delete newData;
                        return ENGINE_E2BIG;
                    }
                    char *newValue = (char *) newData->getBlob();
                    std::memcpy(newValue, value->getBlob(),
                                FLEX_DATA_OFFSET + value->getExtLen());
                    std::memcpy(newValue + FLEX_DATA_OFFSET + value->getExtLen(),
                                compOutput.buf.get(), compOutput.len);
                    value.reset(newData);
                    return ENGINE_SUCCESS;
                }
            }
        }
    }
    LOG(EXTENSION_LOG_WARNING, "Unsupported operation (Append) :: "
            "Original Data of type: %u, Appending data of type: %u",
            value->getDataType(), i.getDataType());
    return ENGINE_FAILED;
}

/**
 * Prepend another item to this item
 *
 * @param itm the item to prepend to this one
 * @param maxItemSize maximum item size permitted
 * @return true if success
 */
ENGINE_ERROR_CODE Item::prepend(const Item &i, size_t maxItemSize) {
    cb_assert(value.get() != NULL);
    cb_assert(i.getValue().get() != NULL);

    switch(value->getDataType()) {
    case PROTOCOL_BINARY_RAW_BYTES:
    case PROTOCOL_BINARY_DATATYPE_JSON:
        {
            switch(i.getDataType()) {
            case PROTOCOL_BINARY_RAW_BYTES:
            case PROTOCOL_BINARY_DATATYPE_JSON:
                // PREPEND NEW TO ORIGINAL AS IS
                {
                    size_t newSize = value->vlength() + i.getValue()->vlength();
                    Blob *newData = Blob::New(newSize, value->getExtLen());
                    if (!newData) {
                        return ENGINE_ENOMEM;
                    } else if (newData->length() > maxItemSize) {
                        LOG(EXTENSION_LOG_WARNING,
                                "New Item size is greater than permitted value "
                                "size, aborting operation (Prepend)");
                        delete newData;
                        return ENGINE_E2BIG;
                    }
                    char *newValue = (char *) newData->getBlob();
                    std::memcpy(newValue, value->getBlob(),
                                FLEX_DATA_OFFSET + value->getExtLen());
                    std::memcpy(newValue + FLEX_DATA_OFFSET +
                                                value->getExtLen(),
                                i.getValue()->getData(),
                                i.getValue()->vlength());
                    std::memcpy(newValue + i.getValue()->length(),
                                value->getData(),
                                value->vlength());
                    value.reset(newData);
                    return ENGINE_SUCCESS;
                }
            case PROTOCOL_BINARY_DATATYPE_COMPRESSED:
            case PROTOCOL_BINARY_DATATYPE_COMPRESSED_JSON:
                //UNCOMPRESS NEW AND PREPEND
                {
                    snap_buf output;
                    snap_ret_t ret = doSnappyUncompress(i.getValue()->getData(),
                                                        i.getValue()->vlength(),
                                                        output);
                    if (ret == SNAP_FAILURE) {
                        LOG(EXTENSION_LOG_WARNING, "(Prepend) Snappy uncompression "
                            "for prepend value of key: %s failed!",
                            i.getKey().c_str());
                        return ENGINE_FAILED;
                    }
                    size_t newSize = value->vlength() + output.len;
                    Blob *newData = Blob::New(newSize, value->getExtLen());
                    if (!newData) {
                        return ENGINE_ENOMEM;
                    } else if (newData->length() > maxItemSize) {
                        LOG(EXTENSION_LOG_WARNING,
                                "New Item size is greater than permitted value "
                                "size, aborting operation (Prepend)");
                        delete newData;
                        return ENGINE_E2BIG;
                    }
                    char *newValue = (char *) newData->getBlob();
                    std::memcpy(newValue, value->getBlob(),
                                FLEX_DATA_OFFSET + value->getExtLen());
                    std::memcpy(newValue + FLEX_DATA_OFFSET +
                                        value->getExtLen(),
                                output.buf.get(), output.len);
                    std::memcpy(newValue + FLEX_DATA_OFFSET +
                                        value->getExtLen() + output.len,
                                value->getData(), value->vlength());
                    value.reset(newData);
                    return ENGINE_SUCCESS;
                }
            }
        }
    case PROTOCOL_BINARY_DATATYPE_COMPRESSED:
    case PROTOCOL_BINARY_DATATYPE_COMPRESSED_JSON:
        {
            switch(i.getDataType()) {
            case PROTOCOL_BINARY_RAW_BYTES:
            case PROTOCOL_BINARY_DATATYPE_JSON:
                // UNCOMPRESS ORIGINAL AND PREPEND NEW, COMPRESS EVERYTHING
                {
                    snap_buf output;
                    snap_ret_t ret = doSnappyUncompress(value->getData(),
                                                        value->vlength(),
                                                        output);
                    if (ret == SNAP_FAILURE) {
                        LOG(EXTENSION_LOG_WARNING, "(Prepend) Snappy uncompression "
                            "for original value of key: %s failed!",
                            i.getKey().c_str());
                        return ENGINE_FAILED;
                    }
                    size_t len = output.len + i.getValue()->vlength();
                    std::unique_ptr<char[]> buf(new char[len]);
                    if (!buf) {
                        return ENGINE_ENOMEM;
                    }
                    std::memcpy(buf.get(), i.getValue()->getData(),
                                i.getValue()->vlength());
                    std::memcpy(buf.get() + i.getValue()->vlength(),
                                output.buf.get(), output.len);

                    snap_buf compOutput;
                    ret = doSnappyCompress(buf.get(), len, compOutput);
                    if (ret == SNAP_FAILURE) {
                        LOG(EXTENSION_LOG_WARNING, "(Prepend) Snappy compression "
                            "for new value of key: %s failed!",
                            i.getKey().c_str());
                        return ENGINE_FAILED;
                    }
                    Blob *newData = Blob::New(compOutput.len, value->getExtLen());
                    if (!newData) {
                        return ENGINE_ENOMEM;
                    } else if (newData->length() > maxItemSize) {
                        LOG(EXTENSION_LOG_WARNING,
                                "New Item size is greater than permitted value "
                                "size, aborting operation (Prepend)");
                        delete newData;
                        return ENGINE_E2BIG;
                    }
                    char *newValue = (char *) newData->getBlob();
                    std::memcpy(newValue, value->getBlob(),
                                FLEX_DATA_OFFSET + value->getExtLen());
                    std::memcpy(newValue + FLEX_DATA_OFFSET + value->getExtLen(),
                                compOutput.buf.get(), compOutput.len);
                    value.reset(newData);
                    return ENGINE_SUCCESS;
                }
            case PROTOCOL_BINARY_DATATYPE_COMPRESSED:
            case PROTOCOL_BINARY_DATATYPE_COMPRESSED_JSON:
                // UNCOMPRESS ORIGINAL AND UNCOMPRESS NEW,
                // THEN PREPEND AND COMPRESS EVERYTHING
                {
                    snap_buf output1;
                    snap_ret_t ret = doSnappyUncompress(value->getData(),
                                                        value->vlength(),
                                                        output1);
                    if (ret == SNAP_FAILURE) {
                        LOG(EXTENSION_LOG_WARNING, "(Prepend) Snappy uncompression "
                            "for original value of key: %s failed!",
                            i.getKey().c_str());
                        return ENGINE_FAILED;
                    }
                    snap_buf output2;
                    ret = doSnappyUncompress(i.getValue()->getData(),
                                             i.getValue()->vlength(),
                                             output2);
                    if (ret == SNAP_FAILURE) {
                        LOG(EXTENSION_LOG_WARNING, "(Prepend) Snappy uncompression "
                            "for prepend value of key: %s failed!",
                            i.getKey().c_str());
                        return ENGINE_FAILED;
                    }
                    size_t len = output1.len + output2.len;
                    std::unique_ptr<char[]> buf(new char[len]);
                    if (!buf) {
                        return ENGINE_ENOMEM;
                    }
                    std::memcpy(buf.get(), output2.buf.get(), output2.len);
                    std::memcpy(buf.get() + output2.len, output1.buf.get(), output1.len);

                    snap_buf compOutput;
                    ret = doSnappyCompress(buf.get(), len, compOutput);
                    if (ret == SNAP_FAILURE) {
                        LOG(EXTENSION_LOG_WARNING, "(Prepend) Snappy compression "
                            "for new value of key: %s failed!",
                            i.getKey().c_str());
                        return ENGINE_FAILED;
                    }
                    Blob *newData = Blob::New(compOutput.len, value->getExtLen());
                    if (!newData) {
                        return ENGINE_ENOMEM;
                    } else if (newData->length() > maxItemSize) {
                        LOG(EXTENSION_LOG_WARNING,
                                "New Item size is greater than permitted value "
                                "size, aborting operation (Prepend)");
                        delete newData;
                        return ENGINE_E2BIG;
                    }
                    char *newValue = (char *) newData->getBlob();
                    std::memcpy(newValue, value->getBlob(),
                                FLEX_DATA_OFFSET + value->getExtLen());
                    std::memcpy(newValue + FLEX_DATA_OFFSET +
                                                value->getExtLen(),
                                compOutput.buf.get(), compOutput.len);
                    value.reset(newData);
                    return ENGINE_SUCCESS;
                }
            }
        }
    }
    LOG(EXTENSION_LOG_WARNING, "Unsupported operation (Prepend)"
            "Original Data of type: %u, Prepending data of type: %u",
            value->getDataType(), i.getDataType());
    return ENGINE_FAILED;
}
