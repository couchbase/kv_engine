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
#include "engine_wrapper.h"
#include <daemon/mcaudit.h>
#include <daemon/memcached.h>
#include <mcbp/protocol/header.h>
#include <tracing/trace_helpers.h>
#include <utilities/protocol2text.h>

ENGINE_ERROR_CODE bucket_unknown_command(Cookie& cookie,
                                         ADD_RESPONSE response) {
    auto& c = cookie.getConnection();
    const void* const_ptr = static_cast<const void*>(cookie.getPacket().data());
    auto* ptr = const_cast<void*>(const_ptr);
    auto* req = reinterpret_cast<protocol_binary_request_header*>(ptr);
    auto ret = c.getBucketEngine()->unknown_command(c.getBucketEngineAsV0(),
                                                    &cookie,
                                                    req,
                                                    response,
                                                    c.getDocNamespace());
    if (ret == ENGINE_DISCONNECT) {
        const auto request = cookie.getRequest();
        LOG_INFO(&c,
                 "%u: %s %s return ENGINE_DISCONNECT",
                 c.getId(),
                 c.getDescription().c_str(),
                 to_string(request.getClientOpcode()).c_str());
    }
    return ret;
}

void bucket_item_set_cas(Cookie& cookie,
                         gsl::not_null<item*> it,
                         uint64_t cas) {
    auto& c = cookie.getConnection();
    c.getBucketEngine()->item_set_cas(c.getBucketEngineAsV0(), it, cas);
}

void bucket_reset_stats(Cookie& cookie) {
    auto& c = cookie.getConnection();
    c.getBucketEngine()->reset_stats(c.getBucketEngineAsV0(), &cookie);
}

bool bucket_get_item_info(Cookie& cookie,
                          gsl::not_null<const item*> item_,
                          gsl::not_null<item_info*> item_info_) {
    auto& c = cookie.getConnection();
    auto ret = c.getBucketEngine()->get_item_info(
            c.getBucketEngineAsV0(), item_, item_info_);
    if (!ret) {
        LOG_INFO(&c,
                 "%u: %s bucket_get_item_info failed",
                 c.getId(),
                 c.getDescription().c_str());
    }

    return ret;
}

cb::EngineErrorMetadataPair bucket_get_meta(Cookie& cookie,
                                            const DocKey& key,
                                            uint16_t vbucket) {
    TRACE_SCOPE(get_server_api(), &cookie, cb::tracing::TraceCode::GETMETA);
    auto& c = cookie.getConnection();
    auto ret = c.getBucketEngine()->get_meta(
            c.getBucketEngineAsV0(), &cookie, key, vbucket);
    if (ret.first == cb::engine_errc::disconnect) {
        LOG_INFO(&c,
                 "%u: %s bucket_get_meta return ENGINE_DISCONNECT",
                 c.getId(),
                 c.getDescription().c_str());
    }

    return ret;
}

ENGINE_ERROR_CODE bucket_store(Cookie& cookie,
                               gsl::not_null<item*> item_,
                               uint64_t& cas,
                               ENGINE_STORE_OPERATION operation,
                               DocumentState document_state) {
    TRACE_SCOPE(get_server_api(), &cookie, cb::tracing::TraceCode::STORE);
    auto& c = cookie.getConnection();
    auto ret = c.getBucketEngine()->store(c.getBucketEngineAsV0(),
                                          &cookie,
                                          item_,
                                          cas,
                                          operation,
                                          document_state);
    if (ret == ENGINE_SUCCESS) {
        using namespace cb::audit::document;
        add(cookie,
            document_state == DocumentState::Alive ? Operation::Modify
                                                   : Operation::Delete);
    } else if (ret == ENGINE_DISCONNECT) {
        LOG_INFO(&c,
                 "%u: %s bucket_store return ENGINE_DISCONNECT",
                 c.getId(),
                 c.getDescription().c_str());
    }

    return ret;
}

cb::EngineErrorCasPair bucket_store_if(Cookie& cookie,
                                       gsl::not_null<item*> item_,
                                       uint64_t cas,
                                       ENGINE_STORE_OPERATION operation,
                                       cb::StoreIfPredicate predicate,
                                       DocumentState document_state) {
    TRACE_SCOPE(get_server_api(), &cookie, cb::tracing::TraceCode::STOREIF);
    auto& c = cookie.getConnection();
    auto ret = c.getBucketEngine()->store_if(c.getBucketEngineAsV0(),
                                             &cookie,
                                             item_,
                                             cas,
                                             operation,
                                             predicate,
                                             document_state);
    if (ret.status == cb::engine_errc::success) {
        using namespace cb::audit::document;
        add(cookie,
            document_state == DocumentState::Alive ? Operation::Modify
                                                   : Operation::Delete);
    } else if (ret.status == cb::engine_errc::disconnect) {
        LOG_INFO(&c,
                 "%u: %s store_if return ENGINE_DISCONNECT",
                 c.getId(),
                 c.getDescription().c_str());
    }

    return ret;
}

ENGINE_ERROR_CODE bucket_remove(Cookie& cookie,
                                const DocKey& key,
                                uint64_t& cas,
                                uint16_t vbucket,
                                mutation_descr_t& mut_info) {
    TRACE_SCOPE(get_server_api(), &cookie, cb::tracing::TraceCode::REMOVE);
    auto& c = cookie.getConnection();
    auto ret = c.getBucketEngine()->remove(
            c.getBucketEngineAsV0(), &cookie, key, cas, vbucket, mut_info);
    if (ret == ENGINE_SUCCESS) {
        cb::audit::document::add(cookie,
                                 cb::audit::document::Operation::Delete);
    } else if (ret == ENGINE_DISCONNECT) {
        LOG_INFO(&c,
                 "%u: %s bucket_remove return ENGINE_DISCONNECT",
                 c.getId(),
                 c.getDescription().c_str());
    }
    return ret;
}

cb::EngineErrorItemPair bucket_get(Cookie& cookie,
                                   const DocKey& key,
                                   uint16_t vbucket,
                                   DocStateFilter documentStateFilter) {
    TRACE_SCOPE(get_server_api(), &cookie, cb::tracing::TraceCode::GET);
    auto& c = cookie.getConnection();
    auto ret = c.getBucketEngine()->get(c.getBucketEngineAsV0(),
                                        &cookie,
                                        key,
                                        vbucket,
                                        documentStateFilter);
    if (ret.first == cb::engine_errc::disconnect) {
        LOG_INFO(&c,
                 "%u: %s bucket_get return ENGINE_DISCONNECT",
                 c.getId(),
                 c.getDescription().c_str());
    }
    return ret;
}

cb::EngineErrorItemPair bucket_get_if(
        Cookie& cookie,
        const DocKey& key,
        uint16_t vbucket,
        std::function<bool(const item_info&)> filter) {
    TRACE_SCOPE(get_server_api(), &cookie, cb::tracing::TraceCode::GETIF);
    auto& c = cookie.getConnection();
    auto ret = c.getBucketEngine()->get_if(
            c.getBucketEngineAsV0(), &cookie, key, vbucket, filter);

    if (ret.first == cb::engine_errc::disconnect) {
        LOG_INFO(&c,
                 "%u: %s bucket_get_if return ENGINE_DISCONNECT",
                 c.getId(),
                 c.getDescription().c_str());
    }
    return ret;
}

cb::EngineErrorItemPair bucket_get_and_touch(Cookie& cookie,
                                             const DocKey& key,
                                             uint16_t vbucket,
                                             uint32_t expiration) {
    TRACE_SCOPE(get_server_api(), &cookie, cb::tracing::TraceCode::GAT);
    auto& c = cookie.getConnection();
    auto ret = c.getBucketEngine()->get_and_touch(
            c.getBucketEngineAsV0(), &cookie, key, vbucket, expiration);

    if (ret.first == cb::engine_errc::disconnect) {
        LOG_INFO(&c,
                 "%u: %s bucket_get_and_touch return ENGINE_DISCONNECT",
                 c.getId(),
                 c.getDescription().c_str());
    }
    return ret;
}

cb::EngineErrorItemPair bucket_get_locked(Cookie& cookie,
                                          const DocKey& key,
                                          uint16_t vbucket,
                                          uint32_t lock_timeout) {
    TRACE_SCOPE(get_server_api(), &cookie, cb::tracing::TraceCode::GETLOCKED);
    auto& c = cookie.getConnection();
    auto ret = c.getBucketEngine()->get_locked(
            c.getBucketEngineAsV0(), &cookie, key, vbucket, lock_timeout);

    if (ret.first == cb::engine_errc::success) {
        cb::audit::document::add(cookie, cb::audit::document::Operation::Lock);
    } else if (ret.first == cb::engine_errc::disconnect) {
        LOG_INFO(&c,
                 "%u: %s bucket_get_locked return ENGINE_DISCONNECT",
                 c.getId(),
                 c.getDescription().c_str());
    }
    return ret;
}

ENGINE_ERROR_CODE bucket_unlock(Cookie& cookie,
                                const DocKey& key,
                                uint16_t vbucket,
                                uint64_t cas) {
    TRACE_SCOPE(get_server_api(), &cookie, cb::tracing::TraceCode::UNLOCK);
    auto& c = cookie.getConnection();
    auto ret = c.getBucketEngine()->unlock(
            c.getBucketEngineAsV0(), &cookie, key, vbucket, cas);
    if (ret == ENGINE_DISCONNECT) {
        LOG_INFO(&c,
                 "%u: %s bucket_unlock return ENGINE_DISCONNECT",
                 c.getId(),
                 c.getDescription().c_str());
    }
    return ret;
}

std::pair<cb::unique_item_ptr, item_info> bucket_allocate_ex(
        Cookie& cookie,
        const DocKey& key,
        const size_t nbytes,
        const size_t priv_nbytes,
        const int flags,
        const rel_time_t exptime,
        uint8_t datatype,
        uint16_t vbucket) {
    TRACE_SCOPE(get_server_api(), &cookie, cb::tracing::TraceCode::ALLOCATE);
    // MB-25650 - We've got a document of 0 byte value and claims to contain
    //            xattrs.. that's not possible.
    if (nbytes == 0 && !mcbp::datatype::is_raw(datatype)) {
        throw cb::engine_error(cb::engine_errc::invalid_arguments,
                               "bucket_allocate_ex: Can't set datatype to " +
                               mcbp::datatype::to_string(datatype) +
                               " for a 0 sized body");
    }

    if (priv_nbytes > COUCHBASE_MAX_ITEM_PRIVILEGED_BYTES) {
        throw cb::engine_error(cb::engine_errc::too_big,
                               "bucket_allocate_ex: privileged bytes " +
                               std::to_string(priv_nbytes) +
                               " exeeds max limit of " + std::to_string(
                                   COUCHBASE_MAX_ITEM_PRIVILEGED_BYTES));
    }

    auto& c = cookie.getConnection();
    try {
        return c.getBucketEngine()->allocate_ex(c.getBucketEngineAsV0(),
                                                &cookie,
                                                key,
                                                nbytes,
                                                priv_nbytes,
                                                flags,
                                                exptime,
                                                datatype,
                                                vbucket);
    } catch (const cb::engine_error& err) {
        if (err.code() == cb::engine_errc::disconnect) {
            LOG_INFO(&c,
                     "%u: %s bucket_allocate_ex return ENGINE_DISCONNECT",
                     c.getId(),
                     c.getDescription().c_str());
        }
        throw err;
    }
}

ENGINE_ERROR_CODE bucket_flush(Cookie& cookie) {
    TRACE_SCOPE(get_server_api(), &cookie, cb::tracing::TraceCode::FLUSH);

    auto& c = cookie.getConnection();
    auto ret = c.getBucketEngine()->flush(c.getBucketEngineAsV0(), &cookie);
    if (ret == ENGINE_DISCONNECT) {
        LOG_INFO(&c,
                 "%u: %s bucket_flush return ENGINE_DISCONNECT",
                 c.getId(),
                 c.getDescription().c_str());
    }
    return ret;
}

ENGINE_ERROR_CODE bucket_get_stats(Cookie& cookie,
                                   cb::const_char_buffer key,
                                   ADD_STAT add_stat) {
    TRACE_SCOPE(get_server_api(), &cookie, cb::tracing::TraceCode::GETSTATS);
    auto& c = cookie.getConnection();
    auto ret = c.getBucketEngine()->get_stats(
            c.getBucketEngineAsV0(), &cookie, key, add_stat);
    if (ret == ENGINE_DISCONNECT) {
        LOG_INFO(&c,
                 "%u: %s bucket_get_stats return ENGINE_DISCONNECT",
                 c.getId(),
                 c.getDescription().c_str());
    }
    return ret;
}
