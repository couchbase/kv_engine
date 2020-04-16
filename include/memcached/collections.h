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
#include "protocol_binary.h"

namespace cb {

struct EngineErrorGetCollectionIDResult {
    /// special case constructor which allows for success with no other data
    /// used in stats path only
    struct allowSuccess {};
    EngineErrorGetCollectionIDResult(engine_errc result, allowSuccess)
        : result(result) {
    }

    /// construct for an error
    explicit EngineErrorGetCollectionIDResult(engine_errc result)
        : result(result) {
        Expects(result != cb::engine_errc::success);
    }

    /// construct for unknown collection or unknown scope error
    EngineErrorGetCollectionIDResult(engine_errc result, uint64_t manifestId)
        : result(result), manifestId(manifestId) {
        Expects(result == cb::engine_errc::unknown_collection ||
                result == cb::engine_errc::unknown_scope);
    }

    /// construct for successful get
    EngineErrorGetCollectionIDResult(uint64_t manifestId,
                                     ScopeID scopeId,
                                     CollectionID collectionId)
        : result(cb::engine_errc::success),
          manifestId(manifestId),
          scopeId(scopeId),
          collectionId(collectionId) {
    }

    uint64_t getManifestId() const {
        return manifestId;
    }

    CollectionID getCollectionId() const {
        return collectionId;
    }

    ScopeID getScopeId() const {
        return scopeId;
    }

    mcbp::request::GetCollectionIDPayload getPayload() const {
        return {manifestId, collectionId};
    }

    engine_errc result;
    uint64_t manifestId{0};
    ScopeID scopeId{ScopeID::Default};
    CollectionID collectionId{CollectionID::Default};
};

struct EngineErrorGetScopeIDResult {
    /// special case constructor which allows for success with no other data
    /// used in stats path only
    struct allowSuccess {};
    EngineErrorGetScopeIDResult(engine_errc result, allowSuccess)
        : result(result) {
    }

    /// construct for an error
    explicit EngineErrorGetScopeIDResult(engine_errc result) : result(result) {
        Expects(result != cb::engine_errc::success);
    }

    /// construct for an unknown_scope
    explicit EngineErrorGetScopeIDResult(uint64_t manifestId)
        : result(cb::engine_errc::unknown_scope), manifestId(manifestId) {
    }

    /// construct for successful get
    EngineErrorGetScopeIDResult(uint64_t manifestId, ScopeID scopeId)
        : result(cb::engine_errc::success),
          manifestId(manifestId),
          scopeId(scopeId) {
    }

    uint64_t getManifestId() const {
        return manifestId;
    }

    ScopeID getScopeId() const {
        return scopeId;
    }

    mcbp::request::GetScopeIDPayload getPayload() const {
        return {manifestId, scopeId};
    }

    engine_errc result;
    uint64_t manifestId{0};
    ScopeID scopeId{ScopeID::Default};
};
}
