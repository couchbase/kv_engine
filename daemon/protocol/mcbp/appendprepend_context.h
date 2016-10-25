/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2016 Couchbase, Inc.
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

#include <platform/compress.h>
#include "../../memcached.h"

/**
 * The AppendPrependCommandContext is a state machine used by the memcached
 * core to implement append and prepend by fetching the document from the
 * underlying engine, performing the operation and try to use CAS to replace
 * the document in the underlying engine. Multiple clients operating on the
 * same document will be detected by the CAS store operation returning EEXISTS,
 * and we just retry the operation.
 */
class AppendPrependCommandContext : public CommandContext {
public:
    /**
     * The internal state machine used to implement the append / prepend
     * operation in the core rather than having each backend try to
     * implement it.
     */
    enum class State : uint8_t {
        // If the client sends compressed data we need to inflate the
        // input data before we can do anything
            InflateInputData,
        // Look up the item to operate on
            GetItem,
        // Allocate the destination object
            AllocateNewItem,
        // Store the new document
            StoreItem,
        // Release all allocated resources. The reason we've got a separate
        // state for this and not using the destructor for this is that
        // we try to store the newly created document with a CAS operation
        // and we might have a race with another client.
            Reset,
        // We're all done :)
            Done
    };

    enum class Mode : uint8_t {
        Append,
        Prepend
    };

    AppendPrependCommandContext(McbpConnection& c,
                                protocol_binary_request_append* req,
                                const Mode &mode_)
        : connection(c),
          mode(mode_),
          key((char*)req->bytes + sizeof(req->bytes),
              ntohs(req->message.header.request.keylen)),
          value(key.buf + key.len, ntohl(req->message.header.request.bodylen) - key.len),
          vbucket(ntohs(req->message.header.request.vbucket)),
          cas(ntohll(req->message.header.request.cas)),
          olditem(nullptr),
          newitem(nullptr),
          state(State::GetItem) {

        auto datatype = req->message.header.request.datatype;
        if (mcbp::datatype::is_compressed(datatype)) {
            state = State::InflateInputData;
        }
    }

    ENGINE_ERROR_CODE step() {
        ENGINE_ERROR_CODE ret;
        do {
            switch (state) {
            case State::InflateInputData:
                ret = inflateInputData();
                break;
            case State::GetItem:
                ret = getItem();
                break;
            case State::AllocateNewItem:
                ret = allocateNewItem();
                break;
            case State::StoreItem:
                ret = storeItem();
                break;
            case State::Reset:
                ret = reset();
                break;
            case State::Done:
                return ENGINE_SUCCESS;
            }
        } while (ret == ENGINE_SUCCESS);

        if (ret != ENGINE_EWOULDBLOCK) {
            SLAB_INCR(&connection, cmd_set, key, nkey);
        }

        return ret;
    }

    ~AppendPrependCommandContext() override {
        reset();
    }

protected:
    ENGINE_ERROR_CODE inflateInputData();

    ENGINE_ERROR_CODE getItem();

    ENGINE_ERROR_CODE allocateNewItem();

    ENGINE_ERROR_CODE storeItem();

    ENGINE_ERROR_CODE reset();

private:
    McbpConnection& connection;
    const Mode mode;
    const const_sized_buffer key;
    const_sized_buffer value;
    const uint16_t vbucket;
    const uint64_t cas;

    item* olditem;
    item_info_holder oldItemInfo;

    item* newitem;
    item_info_holder newItemInfo;

    cb::compression::Buffer buffer;
    cb::compression::Buffer inputbuffer;
    State state;
};
