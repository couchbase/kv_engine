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

#include "item.h"
#include "cJSON.h"
#include <snappy-c.h>

AtomicValue<uint64_t> Item::casCounter(1);
const uint32_t Item::metaDataSize(2*sizeof(uint32_t) + 2*sizeof(uint64_t) + 2);

/**
 * Function to get length of uncompressed value, after un-compression
 */
bool getUnCompressedLength(const char *val, size_t len, size_t *newLen) {
    if (snappy_uncompressed_length(val, len, newLen) != SNAPPY_OK) {
        LOG(EXTENSION_LOG_WARNING, "Failed to inflate item");
        return false;
    }
    return true;
}

/**
 * Function to uncompress a compressed value
 */
bool doUnCompress(const char *buf, size_t len, char *newBuf, size_t *newLen) {
    if (snappy_uncompress(buf, len, newBuf, newLen) != SNAPPY_OK) {
        LOG(EXTENSION_LOG_WARNING, "Failed to inflate item");
        return false;
    }
    return true;
}

/**
 * Function to compress an uncompressed value
 */
bool doCompress(const char *buf, size_t len, char *newBuf, size_t *newLen) {
    if (snappy_compress(buf, len, newBuf, newLen) != SNAPPY_OK) {
        LOG(EXTENSION_LOG_WARNING, "Failed to compress item");
        return false;
    }
    return true;
}

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
                {
                    size_t newSize = value->vlength() + i.getValue()->vlength();
                    Blob *newData = Blob::New(newSize, value->getExtLen());
                    if (newData->length() > maxItemSize) {
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
                {
                    size_t inflated_length;
                    if (!getUnCompressedLength(i.getValue()->getData(),
                                               i.getValue()->vlength(),
                                               &inflated_length)) {
                        return ENGINE_FAILED;
                    }
                    char *buf = (char *) malloc(inflated_length);
                    if (!doUnCompress(i.getValue()->getData(),
                                      i.getValue()->vlength(),
                                      buf, &inflated_length)) {
                        free (buf);
                        return ENGINE_FAILED;
                    }
                    size_t newSize = value->vlength() + inflated_length;
                    Blob *newData = Blob::New(newSize, value->getExtLen());
                    if (newData->length() > maxItemSize) {
                        LOG(EXTENSION_LOG_WARNING,
                                "New Item size is greater than permitted value "
                                "size, aborting operation (Append)");
                        free (buf);
                        delete newData;
                        return ENGINE_E2BIG;
                    }
                    char *newValue = (char *) newData->getBlob();
                    std::memcpy(newValue, value->getBlob(), value->length());
                    std::memcpy(newValue + value->length(), buf,
                                inflated_length);
                    value.reset(newData);
                    free (buf);
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
                {
                    size_t inflated_length;
                    if (!getUnCompressedLength(value->getData(),
                                               value->vlength(),
                                               &inflated_length)) {
                        return ENGINE_FAILED;
                    }
                    char *buf = (char *) malloc(inflated_length +
                                                i.getValue()->vlength());
                    if(!doUnCompress(value->getData(), value->vlength(),
                                     buf, &inflated_length)) {
                        free (buf);
                        return ENGINE_FAILED;
                    }
                    std::memcpy(buf + inflated_length, i.getValue()->getData(),
                            i.getValue()->vlength());
                    size_t newBytes = snappy_max_compressed_length(
                                    inflated_length + i.getValue()->vlength());
                    char *newBuf = (char *) malloc(newBytes);
                    if (!doCompress(buf,
                                    inflated_length + i.getValue()->vlength(),
                                    newBuf, &newBytes)) {
                        free (buf);
                        free (newBuf);
                        return ENGINE_FAILED;
                    }
                    free (buf);
                    Blob *newData = Blob::New(newBytes, value->getExtLen());
                    if (newData->length() > maxItemSize) {
                        LOG(EXTENSION_LOG_WARNING,
                                "New Item size is greater than permitted value "
                                "size, aborting operation (Append)");
                        free (newBuf);
                        delete newData;
                        return ENGINE_E2BIG;
                    }
                    char *newValue = (char *) newData->getBlob();
                    std::memcpy(newValue, value->getBlob(),
                            FLEX_DATA_OFFSET + value->getExtLen());
                    std::memcpy(newValue + FLEX_DATA_OFFSET + value->getExtLen(),
                            newBuf, newBytes);
                    value.reset(newData);
                    free (newBuf);
                    return ENGINE_SUCCESS;
                }
            case PROTOCOL_BINARY_DATATYPE_COMPRESSED:
            case PROTOCOL_BINARY_DATATYPE_COMPRESSED_JSON:
                {
                    size_t inflated_length;
                    if (!getUnCompressedLength(value->getData(),
                                               value->vlength(),
                                               &inflated_length)) {
                        return ENGINE_FAILED;
                    }
                    size_t inflated_length2;
                    if (!getUnCompressedLength(i.getValue()->getData(),
                                               i.getValue()->vlength(),
                                               &inflated_length2)) {
                        return ENGINE_FAILED;
                    }
                    char *buf = (char *) malloc(inflated_length +
                                                inflated_length2);
                    if (!doUnCompress(value->getData(), value->vlength(),
                                      buf, &inflated_length)) {
                        free (buf);
                        return ENGINE_FAILED;
                    }
                    if (!doUnCompress(i.getValue()->getData(),
                                      i.getValue()->vlength(),
                                      buf + inflated_length,
                                      &inflated_length2)) {
                        free (buf);
                        return ENGINE_FAILED;
                    }
                    size_t newBytes = snappy_max_compressed_length(
                                        inflated_length + inflated_length2);
                    char *newBuf = (char *) malloc(newBytes);
                    if(!doCompress(buf, inflated_length + inflated_length2,
                                   newBuf, &newBytes)) {
                        free (buf);
                        free (newBuf);
                        return ENGINE_FAILED;
                    }
                    free (buf);
                    Blob *newData = Blob::New(newBytes, value->getExtLen());
                    if (newData->length() > maxItemSize) {
                        LOG(EXTENSION_LOG_WARNING,
                                "New Item size is greater than permitted value "
                                "size, aborting operation (Append)");
                        free (newBuf);
                        delete newData;
                        return ENGINE_E2BIG;
                    }
                    char *newValue = (char *) newData->getBlob();
                    std::memcpy(newValue, value->getBlob(),
                                FLEX_DATA_OFFSET + value->getExtLen());
                    std::memcpy(newValue + FLEX_DATA_OFFSET + value->getExtLen(),
                                newBuf, newBytes);
                    value.reset(newData);
                    free (newBuf);
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
                {
                    size_t newSize = value->vlength() + i.getValue()->vlength();
                    Blob *newData = Blob::New(newSize, value->getExtLen());
                    if (newData->length() > maxItemSize) {
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
                {
                    size_t inflated_length;
                    if (!getUnCompressedLength(i.getValue()->getData(),
                                               i.getValue()->vlength(),
                                               &inflated_length)) {
                        return ENGINE_FAILED;
                    }
                    char *buf = (char *) malloc(inflated_length);
                    if (!doUnCompress(i.getValue()->getData(),
                                      i.getValue()->vlength(), buf,
                                      &inflated_length)) {
                        free (buf);
                        return ENGINE_FAILED;
                    }
                    size_t newSize = value->vlength() + inflated_length;
                    Blob *newData = Blob::New(newSize, value->getExtLen());
                    if (newData->length() > maxItemSize) {
                        LOG(EXTENSION_LOG_WARNING,
                                "New Item size is greater than permitted value "
                                "size, aborting operation (Prepend)");
                        free (buf);
                        delete newData;
                        return ENGINE_E2BIG;
                    }
                    char *newValue = (char *) newData->getBlob();
                    std::memcpy(newValue, value->getBlob(),
                                FLEX_DATA_OFFSET + value->getExtLen());
                    std::memcpy(newValue + FLEX_DATA_OFFSET +
                                        value->getExtLen(),
                                buf, inflated_length);
                    std::memcpy(newValue + FLEX_DATA_OFFSET +
                                        value->getExtLen() + inflated_length,
                                value->getData(), value->vlength());
                    value.reset(newData);
                    free (buf);
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
                {
                    size_t inflated_length;
                    if (!getUnCompressedLength(value->getData(),
                                               value->vlength(),
                                               &inflated_length)) {
                        return ENGINE_FAILED;
                    }
                    char *buf = (char *) malloc(inflated_length +
                                                i.getValue()->vlength());
                    std::memcpy(buf, i.getValue()->getData(),
                                i.getValue()->vlength());
                    if (!doUnCompress(value->getData(),
                                      value->vlength(),
                                      buf + i.getValue()->vlength(),
                                      &inflated_length)) {
                        free (buf);
                        return ENGINE_FAILED;
                    }
                    size_t newBytes = snappy_max_compressed_length(
                                                    inflated_length +
                                                    i.getValue()->vlength());
                    char *newBuf = (char *) malloc(newBytes);
                    if (!doCompress(buf,
                                    inflated_length + i.getValue()->vlength(),
                                    newBuf, &newBytes)) {
                        free (buf);
                        free (newBuf);
                        return ENGINE_FAILED;
                    }
                    free (buf);
                    Blob *newData = Blob::New(newBytes, value->getExtLen());
                    if (newData->length() > maxItemSize) {
                        LOG(EXTENSION_LOG_WARNING,
                                "New Item size is greater than permitted value "
                                "size, aborting operation (Prepend)");
                        free (newBuf);
                        delete newData;
                        return ENGINE_E2BIG;
                    }
                    char *newValue = (char *) newData->getBlob();
                    std::memcpy(newValue, value->getBlob(),
                            FLEX_DATA_OFFSET + value->getExtLen());
                    std::memcpy(newValue + FLEX_DATA_OFFSET + value->getExtLen(),
                            newBuf, newBytes);
                    value.reset(newData);
                    free (newBuf);
                    return ENGINE_SUCCESS;
                }
            case PROTOCOL_BINARY_DATATYPE_COMPRESSED:
            case PROTOCOL_BINARY_DATATYPE_COMPRESSED_JSON:
                {
                    size_t inflated_length;
                    if (!getUnCompressedLength(value->getData(),
                                               value->vlength(),
                                               &inflated_length)) {
                        return ENGINE_FAILED;
                    }
                    size_t inflated_length2;
                    if (!getUnCompressedLength(i.getValue()->getData(),
                                               i.getValue()->vlength(),
                                               &inflated_length2)) {
                        return ENGINE_FAILED;
                    }
                    char *buf = (char *) malloc(inflated_length +
                                                inflated_length2);
                    if (!doUnCompress(i.getValue()->getData(),
                                      i.getValue()->vlength(),
                                      buf, &inflated_length2)) {
                        free (buf);
                        return ENGINE_FAILED;
                    }
                    if (!doUnCompress(value->getData(),
                                      value->vlength(),
                                      buf + inflated_length2,
                                      &inflated_length)) {
                        free (buf);
                        return ENGINE_FAILED;
                    }
                    size_t newBytes = snappy_max_compressed_length(
                                    inflated_length + inflated_length2);
                    char *newBuf = (char *) malloc(newBytes);
                    if (!doCompress(buf, inflated_length + inflated_length2,
                                    newBuf, &newBytes)) {
                        free (buf);
                        free (newBuf);
                        return ENGINE_FAILED;
                    }
                    free (buf);
                    Blob *newData = Blob::New(newBytes, value->getExtLen());
                    if (newData->length() > maxItemSize) {
                        LOG(EXTENSION_LOG_WARNING,
                                "New Item size is greater than permitted value "
                                "size, aborting operation (Prepend)");
                        free (newBuf);
                        delete newData;
                        return ENGINE_E2BIG;
                    }
                    char *newValue = (char *) newData->getBlob();
                    std::memcpy(newValue, value->getBlob(),
                                FLEX_DATA_OFFSET + value->getExtLen());
                    std::memcpy(newValue + FLEX_DATA_OFFSET +
                                                value->getExtLen(),
                                newBuf, newBytes);
                    value.reset(newData);
                    free (newBuf);
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
