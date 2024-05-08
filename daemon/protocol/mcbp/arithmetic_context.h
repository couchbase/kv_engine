/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2016-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */
#pragma once

#include "steppable_command_context.h"
#include <daemon/memcached.h>
#include <memcached/engine.h>

class ItemDissector;

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

    ArithmeticCommandContext(Cookie& cookie, const cb::mcbp::Request& req);

protected:
    cb::engine_errc step() override {
        auto ret = cb::engine_errc::success;
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
                return cb::engine_errc::success;
            }
        } while (ret == cb::engine_errc::success);

        return ret;
    }

    cb::engine_errc getItem();

    cb::engine_errc createNewItem();

    cb::engine_errc storeNewItem();

    cb::engine_errc allocateNewItem();

    cb::engine_errc storeItem();

    cb::engine_errc sendResult();

    cb::engine_errc reset();

private:

    const cb::mcbp::request::ArithmeticPayload extras;
    const uint64_t cas;
    const Vbid vbucket;
    const bool increment;

    std::unique_ptr<ItemDissector> old_item;

    cb::unique_item_ptr newitem;
    uint64_t result = 0;
    State state = State::GetItem;
};
