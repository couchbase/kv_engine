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

#include <memcached/dockey.h>
#include <memcached/engine.h>
#include <platform/compress.h>
#include "../../memcached.h"
#include "steppable_command_context.h"

/**
 * The AppendPrependCommandContext is a state machine used by the memcached
 * core to implement append and prepend by fetching the document from the
 * underlying engine, performing the operation and try to use CAS to replace
 * the document in the underlying engine. Multiple clients operating on the
 * same document will be detected by the CAS store operation returning EEXISTS,
 * and we just retry the operation.
 */
class AppendPrependCommandContext : public SteppableCommandContext {
public:
    /**
     * The internal state machine used to implement the append / prepend
     * operation in the core rather than having each backend try to
     * implement it.
     */
    enum class State : uint8_t {
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

    enum class Mode { Append, Prepend };

    AppendPrependCommandContext(Cookie& cookie, const cb::mcbp::Request& req);

protected:
    cb::engine_errc step() override;

    cb::engine_errc getItem();

    cb::engine_errc allocateNewItem();

    cb::engine_errc storeItem();

    cb::engine_errc reset();

private:
    const Mode mode;
    const Vbid vbucket;
    const uint64_t cas;

    cb::unique_item_ptr olditem;
    item_info oldItemInfo;

    cb::unique_item_ptr newitem;

    cb::compression::Buffer buffer;
    State state;
};
