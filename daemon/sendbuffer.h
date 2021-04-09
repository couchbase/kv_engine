/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2017-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */
#pragma once

#include <memcached/engine.h>
#include <platform/compression/buffer.h>
class Bucket;

/**
 * Abstract class for a generic send buffer to be passed to libevent
 * which holds some data allocated elsewhere which needs to be released
 * when libevent is done sending the data
 */
class SendBuffer {
public:
    /// Using a SendBuffer have an overhead as we need to allocate memory
    /// to hold it _AND_ it may result into multiple system calls being used
    /// as the underlying system may not fit all of the data into a single
    /// system call (for TLS it gets even worse as it'll result in multiple
    /// TLS frames which add extra CPU cycles and network overhead)
    ///.
    /// A sendbuffer should not be used unless the payload is >4k
    constexpr static std::size_t MinimumDataSize = 4096;

    explicit SendBuffer(std::string_view view) : payload(view) {
    }
    virtual ~SendBuffer() = default;
    virtual std::string_view getPayload() {
        return payload;
    }

protected:
    std::string_view payload;
};

/**
 * Specialized send buffer which holds an item which needs to be
 * released once libevent is done sending any data held by the
 * object.
 */
class ItemSendBuffer : public SendBuffer {
public:
    /**
     * @param itm The item send (the ownership of the item is
     *            transferred to this object)
     * @param view The memory area within the item to transfer
     * @param bucket The bucket the item belongs to
     */
    ItemSendBuffer(cb::unique_item_ptr itm,
                   std::string_view view,
                   Bucket& bucket);

    ~ItemSendBuffer() override;

protected:
    cb::unique_item_ptr item;
    Bucket& bucket;
};

/**
 * Specialized class to send a buffer allocated by the compression
 * framework.
 */
class CompressionSendBuffer : public SendBuffer {
public:
    CompressionSendBuffer(cb::compression::Buffer& buffer,
                          std::string_view view)
        : SendBuffer(view),
          allocator(buffer.allocator),
          data(buffer.release()) {
    }

    ~CompressionSendBuffer() override {
        allocator.deallocate(data);
    }

protected:
    cb::compression::Allocator allocator;
    char* data;
};
