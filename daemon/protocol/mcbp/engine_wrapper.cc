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
#include <utilities/protocol2text.h>
#include <daemon/mcaudit.h>

ENGINE_ERROR_CODE bucket_unknown_command(McbpConnection* c,
                                         protocol_binary_request_header* request,
                                         ADD_RESPONSE response) {
    auto ret = c->getBucketEngine()->unknown_command(c->getBucketEngineAsV0(),
                                                     c->getCookie(),
                                                     request,
                                                     response,
                                                     c->getDocNamespace());
    if (ret == ENGINE_DISCONNECT) {
        LOG_INFO(c,
                 "%u: %s %s return ENGINE_DISCONNECT",
                 c->getId(),
                 c->getDescription().c_str(),
                 memcached_opcode_2_text(c->getCmd()));
    }
    return ret;
}

void bucket_item_set_cas(McbpConnection* c, item* it, uint64_t cas) {
    c->getBucketEngine()->item_set_cas(c->getBucketEngineAsV0(), c->getCookie(),
                                       it, cas);
}

void bucket_reset_stats(McbpConnection* c) {
    c->getBucketEngine()->reset_stats(c->getBucketEngineAsV0(), c->getCookie());
}

bool bucket_get_item_info(McbpConnection* c, const item* item_,
                          item_info* item_info_) {
    auto ret = c->getBucketEngine()->get_item_info(
            c->getBucketEngineAsV0(), c->getCookie(), item_, item_info_);
    if (!ret) {
        LOG_INFO(c,
                 "%u: %s bucket_get_item_info failed",
                 c->getId(),
                 c->getDescription().c_str());
    }

    return ret;
}

cb::EngineErrorMetadataPair bucket_get_meta(McbpConnection* c,
                                            const DocKey& key,
                                            uint16_t vbucket) {
    auto ret = c->getBucketEngine()->get_meta(
            c->getBucketEngineAsV0(), c->getCookie(), key, vbucket);
    if (ret.first == cb::engine_errc::disconnect) {
        LOG_INFO(c,
                 "%u: %s bucket_get_meta return ENGINE_DISCONNECT",
                 c->getId(),
                 c->getDescription().c_str());
    }

    return ret;
}

bool bucket_set_item_info(McbpConnection* c, item* item_,
                          const item_info* item_info_) {
    auto ret = c->getBucketEngine()->set_item_info(
            c->getBucketEngineAsV0(), c->getCookie(), item_, item_info_);

    if (!ret) {
        LOG_INFO(c,
                 "%u: %s bucket_set_item_info failed",
                 c->getId(),
                 c->getDescription().c_str());
    }

    return ret;
}

ENGINE_ERROR_CODE bucket_store(McbpConnection* c,
                               item* item_,
                               uint64_t* cas,
                               ENGINE_STORE_OPERATION operation,
                               DocumentState document_state) {
    auto ret = c->getBucketEngine()->store(c->getBucketEngineAsV0(),
                                           c->getCookie(),
                                           item_,
                                           cas,
                                           operation,
                                           document_state);
    if (ret == ENGINE_SUCCESS) {
        using namespace cb::audit::document;
        add(c->getCookieObject(),
            document_state == DocumentState::Alive ? Operation::Modify
                                                   : Operation::Delete);
    } else if (ret == ENGINE_DISCONNECT) {
        LOG_INFO(c,
                 "%u: %s bucket_store return ENGINE_DISCONNECT",
                 c->getId(),
                 c->getDescription().c_str());
    }

    return ret;
}

cb::EngineErrorCasPair bucket_store_if(McbpConnection* c,
                                       item* item_,
                                       uint64_t cas,
                                       ENGINE_STORE_OPERATION operation,
                                       cb::StoreIfPredicate predicate,
                                       DocumentState document_state) {
    auto ret = c->getBucketEngine()->store_if(c->getBucketEngineAsV0(),
                                              c->getCookie(),
                                              item_,
                                              cas,
                                              operation,
                                              predicate,
                                              document_state);
    if (ret.status == cb::engine_errc::success) {
        using namespace cb::audit::document;
        add(c->getCookieObject(),
            document_state == DocumentState::Alive ? Operation::Modify
                                                   : Operation::Delete);
    } else if (ret.status == cb::engine_errc::disconnect) {
        LOG_INFO(c,
                 "%u: %s store_if return ENGINE_DISCONNECT",
                 c->getId(),
                 c->getDescription().c_str());
    }

    return ret;
}

ENGINE_ERROR_CODE bucket_remove(McbpConnection* c,
                                const DocKey& key,
                                uint64_t* cas,
                                uint16_t vbucket,
                                mutation_descr_t* mut_info) {
    auto ret = c->getBucketEngine()->remove(c->getBucketEngineAsV0(),
                                            c->getCookie(),
                                            key,
                                            cas,
                                            vbucket,
                                            mut_info);
    if (ret == ENGINE_SUCCESS) {
        cb::audit::document::add(c->getCookieObject(),
                                 cb::audit::document::Operation::Delete);
    } else if (ret == ENGINE_DISCONNECT) {
        LOG_INFO(c,
                 "%u: %s bucket_remove return ENGINE_DISCONNECT",
                 c->getId(),
                 c->getDescription().c_str());
    }
    return ret;
}

cb::EngineErrorItemPair bucket_get(McbpConnection* c,
                             const DocKey& key,
                             uint16_t vbucket,
                             DocStateFilter documentStateFilter) {
    auto ret = c->getBucketEngine()->get(c->getBucketEngineAsV0(),
                                         c->getCookie(),
                                         key,
                                         vbucket,
                                         documentStateFilter);
    if (ret.first == cb::engine_errc::disconnect) {
        LOG_INFO(c,
                 "%u: %s bucket_get return ENGINE_DISCONNECT",
                 c->getId(),
                 c->getDescription().c_str());
    }
    return ret;
}

cb::EngineErrorItemPair bucket_get_if(McbpConnection* c,
                                      const DocKey& key,
                                      uint16_t vbucket,
                                      std::function<bool(
                                          const item_info&)> filter) {
    auto ret = c->getBucketEngine()->get_if(
            c->getBucketEngineAsV0(), c->getCookie(), key, vbucket, filter);

    if (ret.first == cb::engine_errc::disconnect) {
        LOG_INFO(c,
                 "%u: %s bucket_get_if return ENGINE_DISCONNECT",
                 c->getId(),
                 c->getDescription().c_str());
    }
    return ret;
}

cb::EngineErrorItemPair bucket_get_and_touch(McbpConnection* c,
                                             const DocKey& key,
                                             uint16_t vbucket,
                                             uint32_t expiration) {
    auto ret = c->getBucketEngine()->get_and_touch(
        c->getBucketEngineAsV0(), c->getCookie(), key, vbucket, expiration);

    if (ret.first == cb::engine_errc::disconnect) {
        LOG_INFO(c,
                 "%u: %s bucket_get_and_touch return ENGINE_DISCONNECT",
                 c->getId(),
                 c->getDescription().c_str());
    }
    return ret;
}

cb::EngineErrorItemPair bucket_get_locked(McbpConnection& c,
                                    const DocKey& key,
                                    uint16_t vbucket,
                                    uint32_t lock_timeout) {
    auto ret = c.getBucketEngine()->get_locked(c.getBucketEngineAsV0(),
                                               c.getCookie(),
                                               key,
                                               vbucket,
                                               lock_timeout);

    if (ret.first == cb::engine_errc::success) {
        cb::audit::document::add(c.getCookieObject(),
                                 cb::audit::document::Operation::Lock);
    } else if (ret.first == cb::engine_errc::disconnect) {
        LOG_INFO(&c,
                 "%u: %s bucket_get_locked return ENGINE_DISCONNECT",
                 c.getId(),
                 c.getDescription().c_str());
    }
    return ret;
}

ENGINE_ERROR_CODE bucket_unlock(McbpConnection& c,
                                const DocKey& key,
                                uint16_t vbucket,
                                uint64_t cas) {
    auto ret = c.getBucketEngine()->unlock(
            c.getBucketEngineAsV0(), c.getCookie(), key, vbucket, cas);
    if (ret == ENGINE_DISCONNECT) {
        LOG_INFO(&c,
                 "%u: %s bucket_unlock return ENGINE_DISCONNECT",
                 c.getId(),
                 c.getDescription().c_str());
    }
    return ret;
}

std::pair<cb::unique_item_ptr, item_info> bucket_allocate_ex(McbpConnection& c,
                                                             const DocKey& key,
                                                             const size_t nbytes,
                                                             const size_t priv_nbytes,
                                                             const int flags,
                                                             const rel_time_t exptime,
                                                             uint8_t datatype,
                                                             uint16_t vbucket) {
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

    try {
        return c.getBucketEngine()->allocate_ex(c.getBucketEngineAsV0(),
                                                c.getCookie(),
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
