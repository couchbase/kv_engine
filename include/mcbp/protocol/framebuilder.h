/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2017 Couchbase, Inc.
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
#pragma once

#include <mcbp/protocol/opcode.h>
#include <mcbp/protocol/request.h>
#include <mcbp/protocol/response.h>
#include <mcbp/protocol/status.h>

namespace cb {
namespace mcbp {

/**
 * FrameBuilder allows you to build up a request / response frame in
 * a provided memory area. It provides helper methods which makes sure
 * that the fields is formatted in the correct byte order etc.
 */
template <typename T>
class FrameBuilder {
public:
    FrameBuilder(cb::byte_buffer backing) : buffer(backing) {
        checkSize(sizeof(T));
        std::fill(backing.begin(), backing.begin() + sizeof(T), 0);
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

    void setVBucket(uint16_t value) {
        getFrame()->setVBucket(value);
    }

    void setOpaque(uint32_t opaque) {
        getFrame()->setOpaque(opaque);
    }

    void setCas(uint64_t val) {
        getFrame()->setCas(val);
    }

    /**
     * Insert the provided extras in the appropriate location (right after
     * the header) and updates the extlen and bodylen field.
     *
     * @param extras the extra field to insert
     */
    void setExtras(cb::const_byte_buffer extras) {
        auto* req = getFrame();

        // can we fit the extras?
        checkSize(req->getExtlen(), extras.size());

        // Start by moving key and body to where it belongs
        const auto old_body_size = req->getBodylen() - req->getExtlen();
        auto* start = buffer.data() + sizeof(*req);
        uint8_t* old_start = start + req->getExtlen();
        uint8_t* new_start = start + extras.size();
        memmove(new_start, old_start, old_body_size);

        // Insert the new extras:
        std::copy(extras.begin(), extras.end(), start);
        req->setExtlen(uint8_t(extras.size()));
        req->setBodylen(old_body_size + extras.size());
    }

    /**
     * Insert the provided key in the appropriate location (right after
     * the extras field) and updates the keylen and bodylen field.
     *
     * @param key the key field to insert
     */
    void setKey(cb::const_byte_buffer key) {
        auto* req = getFrame();
        checkSize(req->getKeylen(), key.size());

        // Start by moving the body to where it belongs
        const auto old_body_size =
                req->getBodylen() - req->getKeylen() - req->getExtlen();
        auto* start = buffer.data() + sizeof(*req) + req->getExtlen();
        uint8_t* old_start = start + req->getKeylen();
        uint8_t* new_start = start + key.size();
        memmove(new_start, old_start, old_body_size);

        // Insert the new key:
        std::copy(key.begin(), key.end(), start);
        req->setKeylen(uint16_t(key.size()));
        req->setBodylen(old_body_size + req->getExtlen() + key.size());
    }

    /**
     * Insert the provided value in the appropriate location (right after
     * the key field) and update the bodylen field.
     *
     * @param value the value to insert into the body
     */
    void setValue(cb::const_byte_buffer value) {
        auto* req = getFrame();

        const auto old_size =
                req->getBodylen() - req->getKeylen() - req->getExtlen();
        checkSize(old_size, value.size());

        auto* start = buffer.data() + sizeof(*req) + req->getExtlen() +
                      req->getKeylen();
        std::copy(value.begin(), value.end(), start);
        req->setBodylen(value.size() + req->getKeylen() + req->getExtlen());
    }

    /**
     * Try to validate the underlying packet
     */
    void validate() {
        getFrame()->validate();
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

    cb::byte_buffer buffer;
};

/**
 * Specialized class to build a Request
 */
class RequestBuilder : public FrameBuilder<Request> {
public:
    RequestBuilder(cb::byte_buffer backing) : FrameBuilder<Request>(backing) {
    }
    void setVbucket(uint16_t vbucket) {
        getFrame()->setVBucket(vbucket);
    }
};

/**
 * Specialized class to build a response
 */
class ResponseBuilder : public FrameBuilder<Response> {
public:
    ResponseBuilder(cb::byte_buffer backing) : FrameBuilder<Response>(backing) {
    }
    void setStatus(Status status) {
        getFrame()->setStatus(uint16_t(status));
    }
};

} // namespace mcbp
} // namespace cb
