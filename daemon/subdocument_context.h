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

#include "connection.h"

#include <cstddef>

/* Struct repesenting a buffer of some known size. This is typically used to
 * refer to some existing region of memory which is owned elsewhere - i.e.
 * a user should not normally be free()ing the buf member themselves!
 */
struct sized_buffer {
    char* buf;
    size_t len;
};

/** Subdoc command context. An instance of this exists for the lifetime of
 *  each subdocument command, and it used to hold information which needs to
 *  persist across calls to subdoc_executor; for example when one or more
 *  engine functions return EWOULDBLOCK and hence the executor needs to be
 *  retried.
 */
struct SubdocCmdContext : public CommandContext {

    SubdocCmdContext(Connection * connection)
      : c(connection),
        in_doc({NULL, 0}),
        in_cas(0),
        executed(false),
        out_doc(NULL) {}

    virtual ~SubdocCmdContext() {
        if (out_doc != NULL) {
            auto engine = c->getBucketEngine();
            engine->release(reinterpret_cast<ENGINE_HANDLE*>(engine), c, out_doc);
        }
    }

    // Static method passed back to memcached to destroy objects of this class.
    static void dtor(Connection * c, void* context);

    // Cookie this command is associated with. Needed for the destructor
    // to release items.
    Connection * c;

    // The expanded input JSON document. This may either refer to the raw engine
    // item iovec; or to the connections' DynamicBuffer if the JSON document
    // had to be decompressed. Either way, it should /not/ be free()d.
    sized_buffer in_doc;

    // CAS value of the input document. Required to ensure we only store a
    // new document which was derived from the same original input document.
    uint64_t in_cas;

    // True if this operation has been successfully executed (via subjson)
    // and we have valid result.
    bool executed;

    // In/Out parameter which contains the result of the executed operation
    Subdoc::Result result;

    // [Mutations only] New item to store into engine. _Must_ be released
    // back to the engine using ENGINE_HANDLE_V1::release()
    item* out_doc;
};
