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

#include <mcbp/protocol/opcode.h>
#include <mcbp/protocol/request.h>
#include <mcbp/protocol/response.h>
#include <mcbp/protocol/status.h>

#include <gsl/gsl-lite.hpp>

namespace cb::mcbp {

/**
 * FrameBuilder allows you to build up a request / response frame in
 * a provided memory area. It provides helper methods which makes sure
 * that the fields is formatted in the correct byte order etc.
 *
 * The frame looks like:
 *
 *
 *     Byte/     0       |       1       |       2       |       3       |
 *        /              |               |               |               |
 *       |0 1 2 3 4 5 6 7|0 1 2 3 4 5 6 7|0 1 2 3 4 5 6 7|0 1 2 3 4 5 6 7|
 *       +---------------+---------------+---------------+---------------+
 *      0| Magic         |               | Framing extras| Key Length    |
 *       +---------------+---------------+---------------+---------------+
 *      4| Extras length |               |                               |
 *       +---------------+---------------+---------------+---------------+
 *      8| Total body length                                             |
 *       +---------------+---------------+---------------+---------------+
 *     12|                                                               |
 *       +---------------+---------------+---------------+---------------+
 *     16|                                                               |
 *       |                                                               |
 *       +---------------+---------------+---------------+---------------+
 *     24| Optional section containing framing extras Max 255 bytes      |
 *       +---------------------------------------------------------------+
 *       | Optional section containing command extras                    |
 *       +---------------------------------------------------------------+
 *       | Optional section containing the key                           |
 *       +---------------------------------------------------------------+
 *       | Optional section containing the value.                        |
 *       | The size of the value is the total body length minus the      |
 *       | size of the optional framing extras, extras and key.          |
 *       +---------------------------------------------------------------+
 *
 */
template <typename T>
class FrameBuilder {
public:
    explicit FrameBuilder(cb::char_buffer backing, bool initialized = false)
        : FrameBuilder(
                  {reinterpret_cast<uint8_t*>(backing.data()), backing.size()},
                  initialized) {
    }

    explicit FrameBuilder(cb::byte_buffer backing, bool initialized = false)
        : buffer(backing) {
        checkSize(sizeof(T));
        if (!initialized) {
            std::fill(backing.begin(), backing.begin() + sizeof(T), 0);
        }
    }

    T* getFrame() {
        return reinterpret_cast<T*>(buffer.data());
    }

    void setMagic(Magic magic) {
        getFrame()->setMagic(magic);
    }

    void setOpcode(ClientOpcode opcode) {
        getFrame()->setOpcode(opcode);
    }

    void setOpcode(ServerOpcode opcode) {
        getFrame()->setOpcode(opcode);
    }

    void setDatatype(Datatype datatype) {
        getFrame()->setDatatype(datatype);
    }

    void setVBucket(Vbid value) {
        getFrame()->setVBucket(value);
    }

    void setOpaque(uint32_t opaque) {
        getFrame()->setOpaque(opaque);
    }

    void setCas(uint64_t val) {
        getFrame()->setCas(val);
    }

    /**
     * Insert/replace the Framing extras section and move the rest of the
     * sections to their new locations.
     */
    void setFramingExtras(cb::const_byte_buffer val) {
        if (!is_alternative_encoding(getFrame()->getMagic())) {
            throw std::logic_error(
                    R"(setFramingExtras: Magic needs to be one of the alternative packet encodings)");
        }
        auto* req = getFrame();
        auto existing = req->getFramingExtras();
        // can we fit the data?
        checkSize(existing.size(), val.size());
        moveAndInsert(existing, val, req->getBodylen() - existing.size());
        req->setFramingExtraslen(gsl::narrow<uint8_t>(val.size()));
    }

    /**
     * Insert/replace the Extras section and move the rest of the sections
     * to their new locations.
     */
    void setExtras(cb::const_byte_buffer val) {
        auto* req = getFrame();
        auto existing = req->getExtdata();
        checkSize(existing.size(), val.size());
        auto move_size = req->getKey().size() + req->getValue().size();
        moveAndInsert(existing, val, move_size);
        req->setExtlen(gsl::narrow<uint8_t>(val.size()));
    }
    void setExtras(std::string_view val) {
        setExtras({reinterpret_cast<const uint8_t*>(val.data()), val.size()});
    }

    /**
     * Insert/replace the Key section and move the value section to the new
     * location.
     */
    void setKey(cb::const_byte_buffer val) {
        auto* req = getFrame();
        auto existing = req->getKey();
        checkSize(existing.size(), val.size());
        moveAndInsert(existing, val, req->getValue().size());
        req->setKeylen(gsl::narrow<uint16_t>(val.size()));
    }

    void setKey(std::string_view val) {
        setKey({reinterpret_cast<const uint8_t*>(val.data()), val.size()});
    }

    /**
     * Insert/replace the value section.
     */
    void setValue(cb::const_byte_buffer val) {
        auto* req = getFrame();
        auto existing = req->getValue();
        checkSize(existing.size(), val.size());
        moveAndInsert(existing, val, 0);
    }

    void setValue(std::string_view val) {
        setValue({reinterpret_cast<const uint8_t*>(val.data()), val.size()});
    }

    /**
     * Try to validate the underlying packet
     */
    void validate() {
        getFrame()->isValid();
    }

protected:
    /**
     * check that the requested size fits into the buffer
     *
     * @param size The total requested size (including header)
     */
    void checkSize(size_t size) {
        if (size > buffer.size()) {
            throw std::logic_error(
                    "FrameBuilder::checkSize: too big to fit in buffer");
        }
    }

    /**
     * check if there is room in the buffer to replace the field with
     * one with a different size
     *
     * @param oldfield
     * @param newfield
     */
    void checkSize(size_t oldfield, size_t newfield) {
        checkSize(sizeof(T) + getFrame()->getBodylen() - oldfield + newfield);
    }

    /**
     * Move the rest of the packet to the new location and insert the
     * new value
     */
    void moveAndInsert(cb::byte_buffer existing,
                       cb::const_byte_buffer value,
                       size_t size) {
        auto* location = existing.data();
        if (size > 0 && existing.size() != value.size()) {
            // we need to move the data following this entry to a different
            // location.
            memmove(location + value.size(), location + existing.size(), size);
        }
        std::copy(value.begin(), value.end(), location);
        auto* req = getFrame();
        // Update the total length
        req->setBodylen(gsl::narrow<uint32_t>(req->getBodylen() -
                                              existing.size() + value.size()));
    }

    cb::byte_buffer buffer;
};

/**
 * Specialized class to build a Request
 */
class RequestBuilder : public FrameBuilder<Request> {
public:
    explicit RequestBuilder(cb::char_buffer backing, bool initialized = false)
        : RequestBuilder(
                  {reinterpret_cast<uint8_t*>(backing.data()), backing.size()},
                  initialized) {
    }
    explicit RequestBuilder(cb::byte_buffer backing, bool initialized = false)
        : FrameBuilder<Request>(backing, initialized) {
    }
    void setVbucket(Vbid vbucket) {
        getFrame()->setVBucket(vbucket);
    }
};

/**
 * Specialized class to build a response
 */
class ResponseBuilder : public FrameBuilder<Response> {
public:
    explicit ResponseBuilder(cb::char_buffer backing, bool initialized = false)
        : ResponseBuilder(
                  {reinterpret_cast<uint8_t*>(backing.data()), backing.size()},
                  initialized) {
    }
    explicit ResponseBuilder(cb::byte_buffer backing, bool initialized = false)
        : FrameBuilder<Response>(backing, initialized) {
    }
    void setStatus(Status status) {
        getFrame()->setStatus(status);
    }
};

} // namespace cb::mcbp
