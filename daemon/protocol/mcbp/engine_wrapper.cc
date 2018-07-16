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

#include <daemon/buckets.h>
#include <daemon/cookie.h>
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
    // @todo: MB-30402 collections broken if enabled
    auto ret = c.getBucketEngine()->unknown_command(
            &cookie, req, response, DocNamespace::DefaultCollection);
    if (ret == ENGINE_DISCONNECT) {
        const auto request = cookie.getRequest();
        LOG_WARNING("{}: {} {} return ENGINE_DISCONNECT",
                    c.getId(),
                    c.getDescription(),
                    to_string(request.getClientOpcode()));
    }
    return ret;
}

void bucket_item_set_cas(Cookie& cookie,
                         gsl::not_null<item*> it,
                         uint64_t cas) {
    auto& c = cookie.getConnection();
    c.getBucketEngine()->item_set_cas(it, cas);
}

void bucket_item_set_datatype(Cookie& cookie,
                              gsl::not_null<item*> it,
                              protocol_binary_datatype_t datatype) {
    auto& c = cookie.getConnection();
    c.getBucketEngine()->item_set_datatype(it, datatype);
}

void bucket_reset_stats(Cookie& cookie) {
    auto& c = cookie.getConnection();
    c.getBucketEngine()->reset_stats(&cookie);
}

bool bucket_get_item_info(Cookie& cookie,
                          gsl::not_null<const item*> item_,
                          gsl::not_null<item_info*> item_info_) {
    auto& c = cookie.getConnection();
    auto ret = c.getBucketEngine()->get_item_info(item_, item_info_);
    if (!ret) {
        LOG_INFO("{}: {} bucket_get_item_info failed",
                 c.getId(),
                 c.getDescription());
    }

    return ret;
}

cb::EngineErrorMetadataPair bucket_get_meta(Cookie& cookie,
                                            const DocKey& key,
                                            uint16_t vbucket) {
    auto& c = cookie.getConnection();
    auto ret = c.getBucketEngine()->get_meta(&cookie, key, vbucket);
    if (ret.first == cb::engine_errc::disconnect) {
        LOG_WARNING("{}: {} bucket_get_meta return ENGINE_DISCONNECT",
                    c.getId(),
                    c.getDescription());
    }

    return ret;
}

ENGINE_ERROR_CODE bucket_store(Cookie& cookie,
                               gsl::not_null<item*> item_,
                               uint64_t& cas,
                               ENGINE_STORE_OPERATION operation,
                               DocumentState document_state) {
    auto& c = cookie.getConnection();
    auto ret = c.getBucketEngine()->store(
            &cookie, item_, cas, operation, document_state);
    if (ret == ENGINE_SUCCESS) {
        using namespace cb::audit::document;
        add(cookie,
            document_state == DocumentState::Alive ? Operation::Modify
                                                   : Operation::Delete);
    } else if (ret == ENGINE_DISCONNECT) {
        LOG_WARNING("{}: {} bucket_store return ENGINE_DISCONNECT",
                    c.getId(),
                    c.getDescription());
    }

    return ret;
}

cb::EngineErrorCasPair bucket_store_if(Cookie& cookie,
                                       gsl::not_null<item*> item_,
                                       uint64_t cas,
                                       ENGINE_STORE_OPERATION operation,
                                       cb::StoreIfPredicate predicate,
                                       DocumentState document_state) {
    auto& c = cookie.getConnection();
    auto ret = c.getBucketEngine()->store_if(
            &cookie, item_, cas, operation, predicate, document_state);
    if (ret.status == cb::engine_errc::success) {
        using namespace cb::audit::document;
        add(cookie,
            document_state == DocumentState::Alive ? Operation::Modify
                                                   : Operation::Delete);
    } else if (ret.status == cb::engine_errc::disconnect) {
        LOG_WARNING("{}: {} store_if return ENGINE_DISCONNECT",
                    c.getId(),
                    c.getDescription());
    }

    return ret;
}

ENGINE_ERROR_CODE bucket_remove(Cookie& cookie,
                                const DocKey& key,
                                uint64_t& cas,
                                uint16_t vbucket,
                                mutation_descr_t& mut_info) {
    auto& c = cookie.getConnection();
    auto ret =
            c.getBucketEngine()->remove(&cookie, key, cas, vbucket, mut_info);
    if (ret == ENGINE_SUCCESS) {
        cb::audit::document::add(cookie,
                                 cb::audit::document::Operation::Delete);
    } else if (ret == ENGINE_DISCONNECT) {
        LOG_WARNING("{}: {} bucket_remove return ENGINE_DISCONNECT",
                    c.getId(),
                    c.getDescription());
    }
    return ret;
}

cb::EngineErrorItemPair bucket_get(Cookie& cookie,
                                   const DocKey& key,
                                   uint16_t vbucket,
                                   DocStateFilter documentStateFilter) {
    auto& c = cookie.getConnection();
    auto ret = c.getBucketEngine()->get(
            &cookie, key, vbucket, documentStateFilter);
    if (ret.first == cb::engine_errc::disconnect) {
        LOG_WARNING("{}: {} bucket_get return ENGINE_DISCONNECT",
                    c.getId(),
                    c.getDescription());
    }
    return ret;
}

BucketCompressionMode bucket_get_compression_mode(Cookie& cookie) {
    auto& c = cookie.getConnection();
    return c.getBucketEngine()->getCompressionMode();
}

float bucket_min_compression_ratio(Cookie& cookie) {
    auto& c = cookie.getConnection();
    return c.getBucketEngine()->getMinCompressionRatio();
}

cb::EngineErrorItemPair bucket_get_if(
        Cookie& cookie,
        const DocKey& key,
        uint16_t vbucket,
        std::function<bool(const item_info&)> filter) {
    auto& c = cookie.getConnection();
    auto ret = c.getBucketEngine()->get_if(&cookie, key, vbucket, filter);

    if (ret.first == cb::engine_errc::disconnect) {
        LOG_WARNING("{}: {} bucket_get_if return ENGINE_DISCONNECT",
                    c.getId(),
                    c.getDescription());
    }
    return ret;
}

cb::EngineErrorItemPair bucket_get_and_touch(Cookie& cookie,
                                             const DocKey& key,
                                             uint16_t vbucket,
                                             uint32_t expiration) {
    auto& c = cookie.getConnection();
    auto ret = c.getBucketEngine()->get_and_touch(
            &cookie, key, vbucket, expiration);

    if (ret.first == cb::engine_errc::disconnect) {
        LOG_WARNING("{}: {} bucket_get_and_touch return ENGINE_DISCONNECT",
                    c.getId(),
                    c.getDescription());
    }
    return ret;
}

cb::EngineErrorItemPair bucket_get_locked(Cookie& cookie,
                                          const DocKey& key,
                                          uint16_t vbucket,
                                          uint32_t lock_timeout) {
    auto& c = cookie.getConnection();
    auto ret = c.getBucketEngine()->get_locked(
            &cookie, key, vbucket, lock_timeout);

    if (ret.first == cb::engine_errc::success) {
        cb::audit::document::add(cookie, cb::audit::document::Operation::Lock);
    } else if (ret.first == cb::engine_errc::disconnect) {
        LOG_WARNING("{}: {} bucket_get_locked return ENGINE_DISCONNECT",
                    c.getId(),
                    c.getDescription());
    }
    return ret;
}

size_t bucket_get_max_item_size(Cookie& cookie) {
    auto& c = cookie.getConnection();
    return c.getBucketEngine()->getMaxItemSize();
}

ENGINE_ERROR_CODE bucket_unlock(Cookie& cookie,
                                const DocKey& key,
                                uint16_t vbucket,
                                uint64_t cas) {
    auto& c = cookie.getConnection();
    auto ret = c.getBucketEngine()->unlock(&cookie, key, vbucket, cas);
    if (ret == ENGINE_DISCONNECT) {
        LOG_WARNING("{}: {} bucket_unlock return ENGINE_DISCONNECT",
                    c.getId(),
                    c.getDescription());
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
        return c.getBucketEngine()->allocate_ex(&cookie,
                                                key,
                                                nbytes,
                                                priv_nbytes,
                                                flags,
                                                exptime,
                                                datatype,
                                                vbucket);
    } catch (const cb::engine_error& err) {
        if (err.code() == cb::engine_errc::disconnect) {
            LOG_WARNING("{}: {} bucket_allocate_ex return ENGINE_DISCONNECT",
                        c.getId(),
                        c.getDescription());
        }
        throw err;
    }
}

ENGINE_ERROR_CODE bucket_flush(Cookie& cookie) {
    auto& c = cookie.getConnection();
    auto ret = c.getBucketEngine()->flush(&cookie);
    if (ret == ENGINE_DISCONNECT) {
        LOG_WARNING("{}: {} bucket_flush return ENGINE_DISCONNECT",
                    c.getId(),
                    c.getDescription());
    }
    return ret;
}

ENGINE_ERROR_CODE bucket_get_stats(Cookie& cookie,
                                   cb::const_char_buffer key,
                                   ADD_STAT add_stat) {
    auto& c = cookie.getConnection();
    auto ret = c.getBucketEngine()->get_stats(&cookie, key, add_stat);
    if (ret == ENGINE_DISCONNECT) {
        LOG_WARNING("{}: {} bucket_get_stats return ENGINE_DISCONNECT",
                    c.getId(),
                    c.getDescription());
    }
    return ret;
}

ENGINE_ERROR_CODE dcpAddStream(Cookie& cookie,
                               uint32_t opaque,
                               uint16_t vbid,
                               uint32_t flags) {
    auto& connection = cookie.getConnection();
    auto* dcp = connection.getBucket().getDcpIface();
    auto ret = dcp->add_stream(&cookie, opaque, vbid, flags);
    if (ret == ENGINE_DISCONNECT) {
        LOG_WARNING("{}: {} dcp.add_stream returned ENGINE_DISCONNECT",
                    connection.getId(),
                    connection.getDescription());
    }
    return ret;
}

ENGINE_ERROR_CODE dcpBufferAcknowledgement(Cookie& cookie,
                                           uint32_t opaque,
                                           uint16_t vbid,
                                           uint32_t ackSize) {
    auto& connection = cookie.getConnection();
    auto* dcp = connection.getBucket().getDcpIface();
    auto ret = dcp->buffer_acknowledgement(&cookie, opaque, vbid, ackSize);
    if (ret == ENGINE_DISCONNECT) {
        LOG_WARNING(
                "{}: {} dcp.buffer_acknowledgement returned ENGINE_DISCONNECT",
                connection.getId(),
                connection.getDescription());
    }
    return ret;
}

ENGINE_ERROR_CODE dcpCloseStream(Cookie& cookie,
                                 uint32_t opaque,
                                 uint16_t vbid) {
    auto& connection = cookie.getConnection();
    auto* dcp = connection.getBucket().getDcpIface();
    auto ret = dcp->close_stream(&cookie, opaque, vbid);
    if (ret == ENGINE_DISCONNECT) {
        LOG_WARNING("{}: {} dcp.close_stream returned ENGINE_DISCONNECT",
                    connection.getId(),
                    connection.getDescription());
    }
    return ret;
}

ENGINE_ERROR_CODE dcpControl(Cookie& cookie,
                             uint32_t opaque,
                             const void* key,
                             uint16_t keySize,
                             const void* value,
                             uint32_t valueSize) {
    auto& connection = cookie.getConnection();
    auto* dcp = connection.getBucket().getDcpIface();
    auto ret = dcp->control(&cookie, opaque, key, keySize, value, valueSize);
    if (ret == ENGINE_DISCONNECT) {
        LOG_WARNING("{}: {} dcp.control returned ENGINE_DISCONNECT",
                    connection.getId(),
                    connection.getDescription());
    }
    return ret;
}

ENGINE_ERROR_CODE dcpDeletion(Cookie& cookie,
                              uint32_t opaque,
                              const DocKey& key,
                              cb::const_byte_buffer value,
                              size_t privilegedPoolSize,
                              uint8_t datatype,
                              uint64_t cas,
                              uint16_t vbid,
                              uint64_t bySeqno,
                              uint64_t revSeqno,
                              cb::const_byte_buffer meta) {
    auto& connection = cookie.getConnection();
    auto* dcp = connection.getBucket().getDcpIface();
    auto ret = dcp->deletion(&cookie,
                             opaque,
                             key,
                             value,
                             privilegedPoolSize,
                             datatype,
                             cas,
                             vbid,
                             bySeqno,
                             revSeqno,
                             meta);
    if (ret == ENGINE_DISCONNECT) {
        LOG_WARNING("{}: {} dcp.deletion returned ENGINE_DISCONNECT",
                    connection.getId(),
                    connection.getDescription());
    }
    return ret;
}

ENGINE_ERROR_CODE dcpDeletionV2(Cookie& cookie,
                                uint32_t opaque,
                                const DocKey& key,
                                cb::const_byte_buffer value,
                                size_t privilegedPoolSize,
                                uint8_t datatype,
                                uint64_t cas,
                                uint16_t vbid,
                                uint64_t bySeqno,
                                uint64_t revSeqno,
                                uint32_t deleteTime) {
    auto& connection = cookie.getConnection();
    auto* dcp = connection.getBucket().getDcpIface();
    auto ret = dcp->deletion_v2(&cookie,
                                opaque,
                                key,
                                value,
                                privilegedPoolSize,
                                datatype,
                                cas,
                                vbid,
                                bySeqno,
                                revSeqno,
                                deleteTime);
    if (ret == ENGINE_DISCONNECT) {
        LOG_WARNING("{}: {} dcp.deletion_v2 returned ENGINE_DISCONNECT",
                    connection.getId(),
                    connection.getDescription());
    }
    return ret;
}

ENGINE_ERROR_CODE dcpExpiration(Cookie& cookie,
                                uint32_t opaque,
                                const DocKey& key,
                                cb::const_byte_buffer value,
                                size_t privilegedPoolSize,
                                uint8_t datatype,
                                uint64_t cas,
                                uint16_t vbid,
                                uint64_t bySeqno,
                                uint64_t revSeqno,
                                cb::const_byte_buffer meta) {
    auto& connection = cookie.getConnection();
    auto* dcp = connection.getBucket().getDcpIface();
    auto ret = dcp->expiration(&cookie,
                               opaque,
                               key,
                               value,
                               privilegedPoolSize,
                               datatype,
                               cas,
                               vbid,
                               bySeqno,
                               revSeqno,
                               meta);
    if (ret == ENGINE_DISCONNECT) {
        LOG_WARNING("{}: {} dcp.expiration returned ENGINE_DISCONNECT",
                    connection.getId(),
                    connection.getDescription());
    }
    return ret;
}

ENGINE_ERROR_CODE dcpFlush(Cookie& cookie, uint32_t opaque, uint16_t vbucket) {
    auto& connection = cookie.getConnection();
    auto* dcp = connection.getBucket().getDcpIface();
    auto ret = dcp->flush(&cookie, opaque, vbucket);
    if (ret == ENGINE_DISCONNECT) {
        LOG_WARNING("{}: {} dcp.flush returned ENGINE_DISCONNECT",
                    connection.getId(),
                    connection.getDescription());
    }
    return ret;
}

ENGINE_ERROR_CODE dcpGetFailoverLog(Cookie& cookie,
                                    uint32_t opaque,
                                    uint16_t vbucket,
                                    dcp_add_failover_log callback) {
    auto& connection = cookie.getConnection();
    auto* dcp = connection.getBucket().getDcpIface();
    auto ret = dcp->get_failover_log(&cookie, opaque, vbucket, callback);
    if (ret == ENGINE_DISCONNECT) {
        LOG_WARNING("{}: {} dcp.get_failover_log returned ENGINE_DISCONNECT",
                    connection.getId(),
                    connection.getDescription());
    }
    return ret;
}

ENGINE_ERROR_CODE dcpMutation(Cookie& cookie,
                              uint32_t opaque,
                              const DocKey& key,
                              cb::const_byte_buffer value,
                              size_t privilegedPoolSize,
                              uint8_t datatype,
                              uint64_t cas,
                              uint16_t vbid,
                              uint32_t flags,
                              uint64_t bySeqno,
                              uint64_t revSeqno,
                              uint32_t expiration,
                              uint32_t lockTime,
                              cb::const_byte_buffer meta,
                              uint8_t nru) {
    auto& connection = cookie.getConnection();
    auto* dcp = connection.getBucket().getDcpIface();
    auto ret = dcp->mutation(&cookie,
                             opaque,
                             key,
                             value,
                             privilegedPoolSize,
                             datatype,
                             cas,
                             vbid,
                             flags,
                             bySeqno,
                             revSeqno,
                             expiration,
                             lockTime,
                             meta,
                             nru);
    if (ret == ENGINE_DISCONNECT) {
        LOG_WARNING("{}: {} dcp.mutation returned ENGINE_DISCONNECT",
                    connection.getId(),
                    connection.getDescription());
    }
    return ret;
}

ENGINE_ERROR_CODE dcpNoop(Cookie& cookie, uint32_t opaque) {
    auto& connection = cookie.getConnection();
    auto* dcp = connection.getBucket().getDcpIface();
    auto ret = dcp->noop(&cookie, opaque);
    if (ret == ENGINE_DISCONNECT) {
        LOG_WARNING("{}: {} dcp.noop returned ENGINE_DISCONNECT",
                    connection.getId(),
                    connection.getDescription());
    }
    return ret;
}

ENGINE_ERROR_CODE dcpOpen(Cookie& cookie,
                          uint32_t opaque,
                          uint32_t seqno,
                          uint32_t flags,
                          cb::const_char_buffer name,
                          cb::const_byte_buffer collectionFilter) {
    auto& connection = cookie.getConnection();
    auto* dcp = connection.getBucket().getDcpIface();
    auto ret = dcp->open(&cookie, opaque, seqno, flags, name, collectionFilter);
    if (ret == ENGINE_DISCONNECT) {
        LOG_WARNING("{}: {} dcp.open returned ENGINE_DISCONNECT",
                    connection.getId(),
                    connection.getDescription());
    }
    return ret;
}

ENGINE_ERROR_CODE dcpSetVbucketState(Cookie& cookie,
                                     uint32_t opaque,
                                     uint16_t vbid,
                                     vbucket_state_t state) {
    auto& connection = cookie.getConnection();
    auto* dcp = connection.getBucket().getDcpIface();
    auto ret = dcp->set_vbucket_state(&cookie, opaque, vbid, state);
    if (ret == ENGINE_DISCONNECT) {
        LOG_WARNING("{}: {} dcp.set_vbucket_state returned ENGINE_DISCONNECT",
                    connection.getId(),
                    connection.getDescription());
    }
    return ret;
}

ENGINE_ERROR_CODE dcpSnapshotMarker(Cookie& cookie,
                                    uint32_t opaque,
                                    uint16_t vbid,
                                    uint64_t startSeqno,
                                    uint64_t endSeqno,
                                    uint32_t flags) {
    auto& connection = cookie.getConnection();
    auto* dcp = connection.getBucket().getDcpIface();
    auto ret = dcp->snapshot_marker(
            &cookie, opaque, vbid, startSeqno, endSeqno, flags);
    if (ret == ENGINE_DISCONNECT) {
        LOG_WARNING("{}: {} dcp.snapshot_marker returned ENGINE_DISCONNECT",
                    connection.getId(),
                    connection.getDescription());
    }
    return ret;
}

ENGINE_ERROR_CODE dcpStreamEnd(Cookie& cookie,
                               uint32_t opaque,
                               uint16_t vbucket,
                               uint32_t flags) {
    auto& connection = cookie.getConnection();
    auto* dcp = connection.getBucket().getDcpIface();
    auto ret = dcp->stream_end(&cookie, opaque, vbucket, flags);
    if (ret == ENGINE_DISCONNECT) {
        LOG_WARNING("{}: {} dcp.stream_end returned ENGINE_DISCONNECT",
                    connection.getId(),
                    connection.getDescription());
    }
    return ret;
}

ENGINE_ERROR_CODE dcpStreamReq(Cookie& cookie,
                               uint32_t flags,
                               uint32_t opaque,
                               uint16_t vbucket,
                               uint64_t startSeqno,
                               uint64_t endSeqno,
                               uint64_t vbucketUuid,
                               uint64_t snapStartSeqno,
                               uint64_t snapEndSeqno,
                               uint64_t* rollbackSeqno,
                               dcp_add_failover_log callback) {
    auto& connection = cookie.getConnection();
    auto* dcp = connection.getBucket().getDcpIface();
    auto ret = dcp->stream_req(&cookie,
                               flags,
                               opaque,
                               vbucket,
                               startSeqno,
                               endSeqno,
                               vbucketUuid,
                               snapStartSeqno,
                               snapEndSeqno,
                               rollbackSeqno,
                               callback);
    if (ret == ENGINE_DISCONNECT) {
        LOG_WARNING("{}: {} dcp.stream_req returned ENGINE_DISCONNECT",
                    connection.getId(),
                    connection.getDescription());
    }
    return ret;
}

ENGINE_ERROR_CODE dcpSystemEvent(Cookie& cookie,
                                 uint32_t opaque,
                                 uint16_t vbucket,
                                 mcbp::systemevent::id eventId,
                                 uint64_t bySeqno,
                                 cb::const_byte_buffer key,
                                 cb::const_byte_buffer eventData) {
    auto& connection = cookie.getConnection();
    auto* dcp = connection.getBucket().getDcpIface();
    auto ret = dcp->system_event(
            &cookie, opaque, vbucket, eventId, bySeqno, key, eventData);
    if (ret == ENGINE_DISCONNECT) {
        LOG_WARNING("{}: {} dcp.system_event returned ENGINE_DISCONNECT",
                    connection.getId(),
                    connection.getDescription());
    }
    return ret;
}
