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

#ifndef MEMCACHED_ENGINE_H
#error "Please include memcached/engine.h instead"
#endif

#include "dockey.h"
#include "engine_common.h"
#include "engine_error.h"
#include <platform/sized_buffer.h>

namespace cb {
using EngineErrorStringPair = std::pair<engine_errc, std::string>;

struct EngineErrorGetCollectionIDResult {
    EngineErrorGetCollectionIDResult(engine_errc result,
                                     uint64_t manifestId,
                                     CollectionID collectionId)
        : result(result), extras(manifestId, collectionId) {
    }

    uint64_t getManifestId() const {
        return htonll(extras.data.manifestId);
    }

    engine_errc result;
    union _extras {
        _extras(uint64_t manifestId, CollectionID collectionId)
            : data(manifestId, collectionId) {
        }
        struct _data {
            _data(uint64_t manifestId, CollectionID collectionId)
                : manifestId(ntohll(manifestId)), collectionId(collectionId) {
            }
            uint64_t manifestId{0};
            CollectionIDNetworkOrder collectionId{0};
        } data;
        char bytes[sizeof(uint64_t) + sizeof(CollectionIDNetworkOrder)];
    } extras;
};
}

struct collections_interface {
    /**
     * Inform the engine of the current collection manifest (a JSON document)
     */
    cb::engine_error (*set_manifest)(gsl::not_null<EngineIface*> handle,
                                     cb::const_char_buffer json);

    /**
     * Retrieve the last manifest set using set_manifest (a JSON document)
     */
    cb::EngineErrorStringPair (*get_manifest)(
            gsl::not_null<EngineIface*> handle);

    cb::EngineErrorGetCollectionIDResult (*get_collection_id)(
            gsl::not_null<EngineIface*> handle,
            gsl::not_null<const void*> cookie,
            cb::const_char_buffer path);
};
