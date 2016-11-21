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

#include  <iomanip>


AtomicValue<uint64_t> Item::casCounter(1);
const uint32_t Item::metaDataSize(2*sizeof(uint32_t) + 2*sizeof(uint64_t) + 2);


std::string to_string(queue_op op) {
    switch(op) {
        case queue_op::set: return "set";
        case queue_op::del: return "del";
        case queue_op::flush: return "flush";
        case queue_op::empty: return "empty";
        case queue_op::checkpoint_start: return "checkpoint_start";
        case queue_op::checkpoint_end: return "checkpoint_end";
        case queue_op::set_vbucket_state: return "set_vbucket_state";
    }
    return "<" +
            std::to_string(static_cast<std::underlying_type<queue_op>::type>(op)) +
            ">";

}

/**
 * Append another item to this item
 *
 * @param itm the item to append to this one
 * @param maxItemSize maximum item size permitted
 * @return true if success
 */
ENGINE_ERROR_CODE Item::append(const Item &i, size_t maxItemSize) {
    if (value.get() == nullptr) {
        throw std::invalid_argument("Item::append: Cannot append to a "
                "NULL value");
    }
    if (i.getValue().get() == nullptr) {
        throw std::invalid_argument("Item::append: Cannot append an Item"
                "with a NULL value to this item");
    }

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
    if (value.get() == nullptr) {
        throw std::invalid_argument("Item::append: Cannot append to a "
                "NULL value");
    }
    if (i.getValue().get() == nullptr) {
        throw std::invalid_argument("Item::append: Cannot append an Item"
                "with a NULL value to this item");
    }

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

bool operator==(const Item& lhs, const Item& rhs) {
    return (lhs.metaData == rhs.metaData) &&
           (*lhs.value == *rhs.value) &&
           (lhs.key == rhs.key) &&
           (lhs.bySeqno == rhs.bySeqno) &&
           // Note: queuedTime is *not* compared. The rationale is it is
           // simply used for stats (measureing queue duration) and hence can
           // be ignored from an "equivilence" pov.
           // (lhs.queuedTime == rhs.queuedTime) &&
           (lhs.vbucketId == rhs.vbucketId) &&
           (lhs.op == rhs.op) &&
           (lhs.nru == rhs.nru);
}

std::ostream& operator<<(std::ostream& os, const Item& i) {
    os << "Item[" << &i << "] with"
       << " key:" << i.key << "\n"
       << "\tvalue:" << *i.value << "\n"
       << "\tmetadata:" << i.metaData << "\n"
       << "\tbySeqno:" << i.bySeqno
       << " queuedTime:" << i.queuedTime
       << " vbucketId:" << i.vbucketId
       << " op:" << to_string(i.op)
       << " nru:" << int(i.nru);
    return os;
}

bool operator==(const ItemMetaData& lhs, const ItemMetaData& rhs) {
    return (lhs.cas == rhs.cas) &&
           (lhs.revSeqno == rhs.revSeqno) &&
           (lhs.flags == rhs.flags) &&
           (lhs.exptime == rhs.exptime);
}

std::ostream& operator<<(std::ostream& os, const ItemMetaData& md) {
    os << "ItemMetaData[" << &md << "] with"
       << " cas:" << md.cas
       << " revSeqno:" << md.revSeqno
       << " flags:" << md.flags
       << " exptime:" << md.exptime;
    return os;
}

bool operator==(const Blob& lhs, const Blob& rhs) {
    return (lhs.size == rhs.size) &&
           (lhs.extMetaLen == rhs.extMetaLen) &&
           (lhs.age == rhs.age) &&
           (memcmp(lhs.data, rhs.data, lhs.size) == 0);
}

std::ostream& operator<<(std::ostream& os, const Blob& b) {
    os << "Blob[" << &b << "] with"
       << " size:" << b.size
       << " extMetaLen:" << int(b.extMetaLen)
       << " age:" << int(b.age)
       << " data: <" << std::hex;
    // Print at most 40 bytes of the body.
    auto bytes_to_print = std::min(uint32_t(40), b.size);
    for (size_t ii = 0; ii < bytes_to_print; ii++) {
        if (ii != 0) {
            os << ' ';
        }
        if (isprint(b.data[ii])) {
            os << b.data[ii];
        } else {
            os << std::setfill('0') << std::setw(2) << int(uint8_t(b.data[ii]));
        }
    }
    os << std::dec << '>';
    return os;
}
