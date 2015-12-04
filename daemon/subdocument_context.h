/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2015 Couchbase, Inc
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

#include "memcached.h"

#include "buffer.h"
#include "subdocument_traits.h"

#include <cstddef>
#include <memory>

/** Subdoc command context. An instance of this exists for the lifetime of
 *  each subdocument command, and it used to hold information which needs to
 *  persist across calls to subdoc_executor; for example when one or more
 *  engine functions return EWOULDBLOCK and hence the executor needs to be
 *  retried.
 */
struct SubdocCmdContext : public CommandContext {

    class OperationSpec;
    typedef std::vector<OperationSpec> Operations;

    SubdocCmdContext(McbpConnection * connection, const SubdocCmdTraits traits_)
      : c(connection),
        traits(traits_),
        in_doc({NULL, 0}),
        in_cas(0),
        executed(false),
        out_doc_len(0),
        out_doc(NULL),
        response_val_len(0) { }

    virtual ~SubdocCmdContext() {
        if (out_doc != NULL) {
            auto engine = c->getBucketEngine();
            engine->release(reinterpret_cast<ENGINE_HANDLE*>(engine), c, out_doc);
        }
    }

    // Returns the total size of all Operation values (bytes).
    uint64_t getOperationValueBytesTotal() const;

    // Cookie this command is associated with. Needed for the destructor
    // to release items.
    McbpConnection* c;

    // The traits for this command.
    SubdocCmdTraits traits;

    // Paths to operate on, one per path in the original request.
    Operations ops;

    // The expanded input JSON document. This may either refer to:
    // a). The raw engine item iovec
    // b). The connections' DynamicBuffer if the JSON document had to be
    //     decompressed.
    // c). {intermediate_result} member of this object.
    // Either way, it should /not/ be free()d.
    // TODO: Remove (b), and just use intermediate result.
    const_sized_buffer in_doc;

    // Temporary buffer used to hold the intermediate result document for
    // multi-path mutations. {in_doc} is then updated to point to this to use
    // as input for the next multi-path mutation.
    std::unique_ptr<char[]> temp_doc;

    // CAS value of the input document. Required to ensure we only store a
    // new document which was derived from the same original input document.
    uint64_t in_cas;

    // True if this operation has been successfully executed (via subjson)
    // and we have valid result.
    bool executed;

    // Overall status of the entire command.
    // For single-path commands this is simply the same as the first (and only)
    // opetation, for multi-path it's an aggregate status.
    protocol_binary_response_status overall_status;

    // [Mutations only] Mutation sequence number and vBucket UUID. Only set
    // if the calling connection has the MUTATION_SEQNO feature enabled; to be
    // included in the response back to the client.
    uint64_t vbucket_uuid;
    uint64_t sequence_no;

    // [Mutations only] Size in bytes of the new item to store into engine.
    // Held in the context so upon success we can update statistics.
    size_t out_doc_len;

    // [Mutations only] New item to store into engine. _Must_ be released
    // back to the engine using ENGINE_HANDLE_V1::release()
    item* out_doc;

    // Size in bytes of the response value to send back to the client.
    size_t response_val_len;

    /* Specification of a single path operation. Encapsulates both the request
     * parameters, and (later) the result of the operation.
     */
    class OperationSpec {
    public:
        // Constructor for lookup operations (no value).
        OperationSpec(SubdocCmdTraits traits_,
                      protocol_binary_subdoc_flag flags,
                      const_sized_buffer path_);

        // Constructor for operations requiring a value.
        OperationSpec(SubdocCmdTraits traits_,
                      protocol_binary_subdoc_flag flags,
                      const_sized_buffer path_,
                      const_sized_buffer value_);

        // Move constructor.
        OperationSpec(OperationSpec&& other);

        // The traits of this individual Operation.
        SubdocCmdTraits traits;

        // Path to operate on. Owned by the original request packet.
        const_sized_buffer path;

        // [For mutations only] Value to apply to document. Owned by the
        // original request packet.
        const_sized_buffer value;

        // Status code of the operation.
        protocol_binary_response_status status;

        // Result of this operation, to be returned back to the client (for
        // operations which return a result).
        Subdoc::Result result;
    };
}; // class SubdocCmdContext

SubdocCmdContext::OperationSpec::OperationSpec(SubdocCmdTraits traits_,
                                               protocol_binary_subdoc_flag flags,
                                               const_sized_buffer path_)
    : SubdocCmdContext::OperationSpec::OperationSpec(traits_, flags, path_,
                                                     {nullptr, 0}) {
}


SubdocCmdContext::OperationSpec::OperationSpec(SubdocCmdTraits traits_,
                                               protocol_binary_subdoc_flag flags,
                                               const_sized_buffer path_,
                                               const_sized_buffer value_)
    : traits(traits_),
      path(path_),
      value(value_),
      status(PROTOCOL_BINARY_RESPONSE_EINTERNAL) {
    if ((flags & SUBDOC_FLAG_MKDIR_P) == SUBDOC_FLAG_MKDIR_P) {
        traits.command = Subdoc::Command
                        (traits.command | Subdoc::Command::FLAG_MKDIR_P);
    }
}


SubdocCmdContext::OperationSpec::OperationSpec(OperationSpec&& other)
    : traits(other.traits),
      path(std::move(other.path)),
      value(std::move(other.value)) {}

uint64_t SubdocCmdContext::getOperationValueBytesTotal() const {
    uint64_t result = 0;
    for (auto& op : ops) {
        result += op.value.len;
    }
    return result;
}
