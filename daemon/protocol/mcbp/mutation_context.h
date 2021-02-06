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

#include "steppable_command_context.h"

#include <memcached/engine.h>
#include <memcached/protocol_binary.h>
#include <platform/compress.h>
#include <xattr/blob.h>

/**
 * The MutationCommandContext is a state machine used by the memcached
 * core to implement ADD, SET and REPLACE.
 */
class MutationCommandContext : public SteppableCommandContext {
public:
    /**
     * The internal state machine used to implement the mutation commands
     * Add, Set, Replace
     */
    enum class State : uint8_t {
        // Validate the input data
        ValidateInput,
        // Get existing item
        GetExistingItemToPreserveXattr,
        // Allocate the destination object
        AllocateNewItem,
        // Store the new document
        StoreItem,
        // Send the response back to the client
        SendResponse,
        // Release all data and start all over again (used by cas collision);-)
        Reset,
        // We're all done :)
        Done
    };

    MutationCommandContext(Cookie& cookie,
                           const cb::mcbp::Request& request,
                           StoreSemantics op_);

    /// this function is the predicate to pass to store_if
    static cb::StoreIfStatus storeIfPredicate(
            const std::optional<item_info>& existing, cb::vbucket_info vb);

protected:
    cb::engine_errc step() override;

    /**
     * Validate the input data provided by the client, and update the
     * datatype for the document if the client isn't datatype aware.
     *
     * @return cb::engine_errc::success if we want to proceed to the next state
     */
    cb::engine_errc validateInput();

    /**
     * Get the existing data from the underlying engine. It may be
     * needed to preserve potential XATTRs on the document
     *
     * @return cb::engine_errc::success if we want to proceed to the next state
     */
    cb::engine_errc getExistingItemToPreserveXattr();

    /**
     * Allocate memory for the object and initialize it with the value
     * provided by the client
     *
     * @return cb::engine_errc::success if we want to proceed to the next state
     */
    cb::engine_errc allocateNewItem();

    /**
     * Store the newly created document in the engine
     *
     * @return cb::engine_errc::success if we want to proceed to the next state
     */
    cb::engine_errc storeItem();

    /**
     * Send the response back to the client (or progress to the next
     * phase for quiet ops).
     *
     * @return cb::engine_errc::success if we want to proceed to the next state
     */
    cb::engine_errc sendResponse();

    /**
     * Release all allocated resources and prepare for a retry
     *
     * @return cb::engine_errc::success if we want to proceed to the next state
     */
    cb::engine_errc reset();

private:
    const StoreSemantics operation;

    const Vbid vbucket;
    const uint64_t input_cas;
    const cb::mcbp::request::MutationPayload extras;

    /// Set to true if the bucket policy enforce the document to be
    /// stored inflated.
    bool mustStoreInflated = false;

    // The datatype for the document to be created.
    // For each field in here, we either trust the client, or re-calculate it
    // server-side:
    //    JSON - ignore what client sends, detect if JSON on the server side.
    //    XATTR - trust what clients sends (as this affects how the value is
    //            parsed).
    protocol_binary_datatype_t datatype;

    // The current state
    State state;

    // The newly created document
    cb::unique_item_ptr newitem;

    // Pointer to the current value stored in the engine
    cb::unique_item_ptr existing;

    // The metadata for the existing item
    item_info existing_info;

    /// The existing XATTRs. Empty if the existing item didn't exist; or
    /// had no XATTRs.
    cb::xattr::Blob existingXattrs;

    /**
     * The predicate function to use for the mutation store, if xattrs are
     * enabled this is initialised to a predicate that will check the existing
     * item's datatype or the vbucket xattr state.
     */
    cb::StoreIfPredicate store_if_predicate;
};
