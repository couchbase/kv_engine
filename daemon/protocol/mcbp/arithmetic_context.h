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
#include <daemon/unique_item_ptr.h>
#include "../../memcached.h"
#include "steppable_command_context.h"

/**
 * The ArithmeticCommandContext is responsible for implementing an
 * increment and decrement operation.
 *
 * The rules are as follows:
 *    * Increment may overflow and wrap
 *    * Decrement always stop at 0
 *    * If the variable isn't found, create it unless exptime is set to
 *      0xffffffff
 */
class ArithmeticCommandContext : public SteppableCommandContext {
public:
    /**
     * The internal state diagram for performing an arithmetic operation.
     * We've got two different paths through the state diagram depending
     * if the counter exists or not:
     *
     * If the document exists:
     *
     *    GetItem -> AllocateNewItem -> StoreItem -> SendResult -> Done
     *
     * If the document doesn't already exists:
     *
     *    GetItem -> CreateNewItem -> StoreNewItem -> SendResult -> Done
     *
     * Each state may terminate the state machine immediately if the
     * underlying engine returns an error. In that case the error is
     * sent back to the client.
     *
     * As a special note if StoreNewItem returns that the key exists (or
     * StoreItem returns EEXISTS) there is a race condition. In that
     * case we restart the operation. To avoid a potential race to run
     * forever we give up after a 10 times.
     */
    enum class State {
        GetItem,
        CreateNewItem,
        StoreNewItem,
        AllocateNewItem,
        StoreItem,
        SendResult,
        Reset,
        Done
    };

    ArithmeticCommandContext(McbpConnection& c,
                             const protocol_binary_request_incr& req)
        : SteppableCommandContext(c),
          key(req.bytes + sizeof(req.bytes),
              ntohs(req.message.header.request.keylen),
              DocNamespace::DefaultCollection),
          request(req),
          cas(ntohll(req.message.header.request.cas)),
          olditem(nullptr, cb::ItemDeleter{c}),
          newitem(nullptr, cb::ItemDeleter{c}),
          vbucket(ntohs(req.message.header.request.vbucket)),
          state(State::GetItem) {
    }

    ~ArithmeticCommandContext() {
        reset();
    }

protected:
    ENGINE_ERROR_CODE step() override {
        ENGINE_ERROR_CODE ret;
        do {
            switch (state) {
            case State::GetItem:
                ret = getItem();
                break;
            case State::AllocateNewItem:
                ret = allocateNewItem();
                break;
            case State::CreateNewItem:
                ret = createNewItem();
                break;
            case State::StoreItem:
                ret = storeItem();
                break;
            case State::StoreNewItem:
                ret = storeNewItem();
                break;
            case State::SendResult:
                ret = sendResult();
                break;
            case State::Reset:
                ret = reset();
                break;
            case State::Done:
                return ENGINE_SUCCESS;
            }
        } while (ret == ENGINE_SUCCESS);

        return ret;
    }

    ENGINE_ERROR_CODE getItem();

    ENGINE_ERROR_CODE createNewItem();

    ENGINE_ERROR_CODE storeNewItem();

    ENGINE_ERROR_CODE allocateNewItem();

    ENGINE_ERROR_CODE storeItem();

    ENGINE_ERROR_CODE sendResult();

    ENGINE_ERROR_CODE reset();

private:

    const DocKey key;
    const protocol_binary_request_incr& request;
    const uint64_t cas;
    cb::unique_item_ptr olditem;
    item_info_holder oldItemInfo;
    cb::unique_item_ptr newitem;
    item_info_holder newItemInfo;
    cb::compression::Buffer buffer;
    uint64_t result;
    const uint16_t vbucket;
    State state;
};
