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
