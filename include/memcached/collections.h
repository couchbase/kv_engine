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

struct EngineErrorGetCollectionIDResult {
    EngineErrorGetCollectionIDResult(engine_errc result,
                                     uint64_t manifestId,
                                     CollectionID collectionId)
        : result(result), extras(manifestId, collectionId) {
    }

    uint64_t getManifestId() const {
        return htonll(extras.data.manifestId);
    }

    CollectionID getCollectionId() const {
        return extras.data.collectionId.to_host();
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

struct EngineErrorGetScopeIDResult {
    EngineErrorGetScopeIDResult(engine_errc result,
                                uint64_t manifestId,
                                ScopeID scopeId)
        : result(result), extras(manifestId, scopeId) {
    }

    uint64_t getManifestId() const {
        return htonll(extras.data.manifestId);
    }

    ScopeID getScopeId() const {
        return extras.data.scopeId.to_host();
    }

    engine_errc result;
    union _extras {
        _extras(uint64_t manifestId, ScopeID scopeId)
            : data(manifestId, scopeId) {
        }
        struct _data {
            _data(uint64_t manifestId, ScopeID scopeId)
                : manifestId(ntohll(manifestId)), scopeId(scopeId) {
            }
            uint64_t manifestId{0};
            ScopeIDNetworkOrder scopeId{0};
        } data;
        char bytes[sizeof(uint64_t) + sizeof(ScopeIDNetworkOrder)];
    } extras;
};
}
