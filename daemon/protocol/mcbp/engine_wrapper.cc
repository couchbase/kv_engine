/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2017-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */
#include "engine_wrapper.h"

#include <daemon/buckets.h>
#include <daemon/cookie.h>
#include <daemon/mcaudit.h>
#include <logger/logger.h>
#include <mcbp/protocol/request.h>
#include <memcached/collections.h>
#include <memcached/durability_spec.h>
#include <memcached/limits.h>
#include <memcached/range_scan_optional_configuration.h>
#include <utilities/logtags.h>

using namespace std::string_literals;

cb::engine_errc bucket_unknown_command(Cookie& cookie,
                                       const AddResponseFn& response) {
    auto& c = cookie.getConnection();
    auto ret = c.getBucketEngine().unknown_command(
            &cookie, cookie.getRequest(), response);
    if (ret == cb::engine_errc::disconnect) {
        const auto request = cookie.getRequest();
        LOG_WARNING("{}: {} {} return cb::engine_errc::disconnect",
                    c.getId(),
                    c.getDescription(),
                    to_string(request.getClientOpcode()));
        c.setTerminationReason("Engine forced disconnect");
    }
    return ret;
}

void bucket_item_set_cas(Connection& c,
                         gsl::not_null<ItemIface*> it,
                         uint64_t cas) {
    c.getBucketEngine().item_set_cas(*it, cas);
}

void bucket_item_set_datatype(Connection& c,
                              gsl::not_null<ItemIface*> it,
                              protocol_binary_datatype_t datatype) {
    c.getBucketEngine().item_set_datatype(*it, datatype);
}

void bucket_reset_stats(Cookie& cookie) {
    auto& c = cookie.getConnection();
    c.getBucketEngine().reset_stats(cookie);
}

bool bucket_get_item_info(Connection& c,
                          gsl::not_null<const ItemIface*> item_,
                          gsl::not_null<item_info*> item_info_) {
    auto ret = c.getBucketEngine().get_item_info(*item_, *item_info_);

    LOG_TRACE("bucket_get_item_info() item:{} -> {}", *item_, ret);

    if (!ret) {
        LOG_INFO("{}: {} bucket_get_item_info failed",
                 c.getId(),
                 c.getDescription());
    }

    return ret;
}

cb::EngineErrorMetadataPair bucket_get_meta(Cookie& cookie,
                                            const DocKey& key,
                                            Vbid vbucket) {
    auto& c = cookie.getConnection();
    auto ret = c.getBucketEngine().get_meta(cookie, key, vbucket);
    if (ret.first == cb::engine_errc::disconnect) {
        LOG_WARNING("{}: {} bucket_get_meta return cb::engine_errc::disconnect",
                    c.getId(),
                    c.getDescription());
        c.setTerminationReason("Engine forced disconnect");
    }

    return ret;
}

cb::engine_errc bucket_store(
        Cookie& cookie,
        gsl::not_null<ItemIface*> item_,
        uint64_t& cas,
        StoreSemantics operation,
        std::optional<cb::durability::Requirements> durability,
        DocumentState document_state,
        bool preserveTtl) {
    auto& c = cookie.getConnection();
    auto ret = c.getBucketEngine().store(cookie,
                                         *item_,
                                         cas,
                                         operation,
                                         durability,
                                         document_state,
                                         preserveTtl);

    LOG_TRACE(
            "bucket_store() item:{} cas:{} op:{} durability:{} doc_state:{} -> "
            "{}",
            *item_,
            cas,
            operation,
            (durability ? to_string(*durability) : "--"),
            document_state,
            ret);

    if (ret == cb::engine_errc::success) {
        using namespace cb::audit::document;
        add(cookie,
            document_state == DocumentState::Alive ? Operation::Modify
                                                   : Operation::Delete);
        cookie.addDocumentWriteBytes(item_->getValueView().size());
    } else if (ret == cb::engine_errc::disconnect) {
        LOG_WARNING("{}: {} bucket_store return cb::engine_errc::disconnect",
                    c.getId(),
                    c.getDescription());
        c.setTerminationReason("Engine forced disconnect");
    }

    return ret;
}

cb::EngineErrorCasPair bucket_store_if(
        Cookie& cookie,
        gsl::not_null<ItemIface*> item_,
        uint64_t cas,
        StoreSemantics operation,
        cb::StoreIfPredicate predicate,
        std::optional<cb::durability::Requirements> durability,
        DocumentState document_state,
        bool preserveTtl) {
    auto& c = cookie.getConnection();
    auto ret = c.getBucketEngine().store_if(cookie,
                                            *item_,
                                            cas,
                                            operation,
                                            predicate,
                                            durability,
                                            document_state,
                                            preserveTtl);
    if (ret.status == cb::engine_errc::success) {
        using namespace cb::audit::document;
        add(cookie,
            document_state == DocumentState::Alive ? Operation::Modify
                                                   : Operation::Delete);
        cookie.addDocumentWriteBytes(item_->getValueView().size());
    } else if (ret.status == cb::engine_errc::disconnect) {
        LOG_WARNING("{}: {} store_if return cb::engine_errc::disconnect",
                    c.getId(),
                    c.getDescription());
        c.setTerminationReason("Engine forced disconnect");
    }

    return ret;
}

cb::engine_errc bucket_remove(
        Cookie& cookie,
        const DocKey& key,
        uint64_t& cas,
        Vbid vbucket,
        std::optional<cb::durability::Requirements> durability,
        mutation_descr_t& mut_info) {
    auto& c = cookie.getConnection();
    auto ret = c.getBucketEngine().remove(
            cookie, key, cas, vbucket, durability, mut_info);
    if (ret == cb::engine_errc::success) {
        cb::audit::document::add(cookie,
                                 cb::audit::document::Operation::Delete);
        // @todo it should be 1 WCU for the tombstone?
        cookie.addDocumentWriteBytes(1);
    } else if (ret == cb::engine_errc::disconnect) {
        LOG_WARNING("{}: {} bucket_remove return cb::engine_errc::disconnect",
                    c.getId(),
                    c.getDescription());
        c.setTerminationReason("Engine forced disconnect");
    }
    return ret;
}

cb::EngineErrorItemPair bucket_get(Cookie& cookie,
                                   const DocKey& key,
                                   Vbid vbucket,
                                   DocStateFilter documentStateFilter) {
    auto& c = cookie.getConnection();
    auto ret =
            c.getBucketEngine().get(cookie, key, vbucket, documentStateFilter);
    if (ret.first == cb::engine_errc::disconnect) {
        LOG_WARNING("{}: {} bucket_get return cb::engine_errc::disconnect",
                    c.getId(),
                    c.getDescription());
        c.setTerminationReason("Engine forced disconnect");
    }
    LOG_TRACE("bucket_get() key:{} vbucket:{} docStateFilter:{} -> {}",
              cb::UserDataView(std::string_view{key}),
              vbucket,
              documentStateFilter,
              ret.first);

    return ret;
}

BucketCompressionMode bucket_get_compression_mode(Cookie& cookie) {
    auto& c = cookie.getConnection();
    return c.getBucketEngine().getCompressionMode();
}

float bucket_min_compression_ratio(Cookie& cookie) {
    auto& c = cookie.getConnection();
    return c.getBucketEngine().getMinCompressionRatio();
}

cb::EngineErrorItemPair bucket_get_if(
        Cookie& cookie,
        const DocKey& key,
        Vbid vbucket,
        std::function<bool(const item_info&)> filter) {
    auto& c = cookie.getConnection();
    auto ret = c.getBucketEngine().get_if(cookie, key, vbucket, filter);

    if (ret.first == cb::engine_errc::disconnect) {
        LOG_WARNING("{}: {} bucket_get_if return cb::engine_errc::disconnect",
                    c.getId(),
                    c.getDescription());
        c.setTerminationReason("Engine forced disconnect");
    }
    return ret;
}

cb::EngineErrorItemPair bucket_get_and_touch(
        Cookie& cookie,
        const DocKey& key,
        Vbid vbucket,
        uint32_t expiration,
        std::optional<cb::durability::Requirements> durability) {
    auto& c = cookie.getConnection();
    auto ret = c.getBucketEngine().get_and_touch(
            cookie, key, vbucket, expiration, durability);

    if (ret.first == cb::engine_errc::disconnect) {
        LOG_WARNING(
                "{}: {} bucket_get_and_touch return "
                "cb::engine_errc::disconnect",
                c.getId(),
                c.getDescription());
        c.setTerminationReason("Engine forced disconnect");
    }
    return ret;
}

cb::EngineErrorItemPair bucket_get_locked(Cookie& cookie,
                                          const DocKey& key,
                                          Vbid vbucket,
                                          uint32_t lock_timeout) {
    auto& c = cookie.getConnection();
    auto ret =
            c.getBucketEngine().get_locked(cookie, key, vbucket, lock_timeout);

    if (ret.first == cb::engine_errc::success) {
        cb::audit::document::add(cookie, cb::audit::document::Operation::Lock);
    } else if (ret.first == cb::engine_errc::disconnect) {
        LOG_WARNING(
                "{}: {} bucket_get_locked return cb::engine_errc::disconnect",
                c.getId(),
                c.getDescription());
        c.setTerminationReason("Engine forced disconnect");
    }
    return ret;
}

size_t bucket_get_max_item_size(Cookie& cookie) {
    auto& c = cookie.getConnection();
    return c.getBucketEngine().getMaxItemSize();
}

cb::engine_errc bucket_unlock(Cookie& cookie,
                              const DocKey& key,
                              Vbid vbucket,
                              uint64_t cas) {
    auto& c = cookie.getConnection();
    auto ret = c.getBucketEngine().unlock(cookie, key, vbucket, cas);
    if (ret == cb::engine_errc::disconnect) {
        LOG_WARNING("{}: {} bucket_unlock return cb::engine_errc::disconnect",
                    c.getId(),
                    c.getDescription());
        c.setTerminationReason("Engine forced disconnect");
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
        Vbid vbucket) {
    // MB-25650 - We've got a document of 0 byte value and claims to contain
    //            xattrs.. that's not possible.
    if (nbytes == 0 && !mcbp::datatype::is_raw(datatype)) {
        throw cb::engine_error(cb::engine_errc::invalid_arguments,
                               "bucket_allocate_ex: Can't set datatype to " +
                               mcbp::datatype::to_string(datatype) +
                               " for a 0 sized body");
    }

    if (priv_nbytes > cb::limits::PrivilegedBytes) {
        throw cb::engine_error(
                cb::engine_errc::too_big,
                "bucket_allocate_ex: privileged bytes " +
                        std::to_string(priv_nbytes) + " exeeds max limit of " +
                        std::to_string(cb::limits::PrivilegedBytes));
    }

    auto& c = cookie.getConnection();
    try {
        LOG_TRACE(
                "bucket_allocate_ex() key:{} nbytes:{} flags:{} exptime:{} "
                "datatype:{} vbucket:{}",
                cb::UserDataView(std::string_view(key)),
                nbytes,
                flags,
                exptime,
                datatype,
                vbucket);

        return c.getBucketEngine().allocateItem(cookie,
                                                key,
                                                nbytes,
                                                priv_nbytes,
                                                flags,
                                                exptime,
                                                datatype,
                                                vbucket);
    } catch (const cb::engine_error& err) {
        if (err.code() == cb::engine_errc::disconnect) {
            LOG_WARNING(
                    "{}: {} bucket_allocate_ex return "
                    "cb::engine_errc::disconnect",
                    c.getId(),
                    c.getDescription());
            c.setTerminationReason("Engine forced disconnect");
        }
        throw err;
    }
}

cb::engine_errc bucket_flush(Cookie& cookie) {
    auto& c = cookie.getConnection();
    auto ret = c.getBucketEngine().flush(cookie);
    if (ret == cb::engine_errc::disconnect) {
        LOG_WARNING("{}: {} bucket_flush return cb::engine_errc::disconnect",
                    c.getId(),
                    c.getDescription());
        c.setTerminationReason("Engine forced disconnect");
    }
    return ret;
}

cb::engine_errc bucket_get_stats(Cookie& cookie,
                                 std::string_view key,
                                 cb::const_byte_buffer value,
                                 const AddStatFn& add_stat) {
    auto& c = cookie.getConnection();
    auto ret = c.getBucketEngine().get_stats(
            cookie,
            key,
            {reinterpret_cast<const char*>(value.data()), value.size()},
            add_stat);
    if (ret == cb::engine_errc::disconnect) {
        LOG_WARNING(
                "{}: {} bucket_get_stats return cb::engine_errc::disconnect",
                c.getId(),
                c.getDescription());
        c.setTerminationReason("Engine forced disconnect");
    }
    return ret;
}

cb::engine_errc dcpAddStream(Cookie& cookie,
                             uint32_t opaque,
                             Vbid vbid,
                             uint32_t flags) {
    auto& connection = cookie.getConnection();
    auto* dcp = connection.getBucket().getDcpIface();
    auto ret = dcp->add_stream(cookie, opaque, vbid, flags);
    if (ret == cb::engine_errc::disconnect) {
        LOG_WARNING(
                "{}: {} dcp.add_stream returned cb::engine_errc::disconnect",
                connection.getId(),
                connection.getDescription());
        connection.setTerminationReason("Engine forced disconnect");
    }
    return ret;
}

cb::engine_errc dcpBufferAcknowledgement(Cookie& cookie,
                                         uint32_t opaque,
                                         Vbid vbid,
                                         uint32_t ackSize) {
    auto& connection = cookie.getConnection();
    auto* dcp = connection.getBucket().getDcpIface();
    auto ret = dcp->buffer_acknowledgement(cookie, opaque, vbid, ackSize);
    if (ret == cb::engine_errc::disconnect) {
        LOG_WARNING(
                "{}: {} dcp.buffer_acknowledgement returned "
                "cb::engine_errc::disconnect",
                connection.getId(),
                connection.getDescription());
        connection.setTerminationReason("Engine forced disconnect");
    }
    return ret;
}

cb::engine_errc dcpCloseStream(Cookie& cookie,
                               uint32_t opaque,
                               Vbid vbid,
                               cb::mcbp::DcpStreamId sid) {
    auto& connection = cookie.getConnection();
    auto* dcp = connection.getBucket().getDcpIface();
    auto ret = dcp->close_stream(cookie, opaque, vbid, sid);
    if (ret == cb::engine_errc::disconnect) {
        LOG_WARNING(
                "{}: {} dcp.close_stream returned cb::engine_errc::disconnect",
                connection.getId(),
                connection.getDescription());
        connection.setTerminationReason("Engine forced disconnect");
    }
    return ret;
}

cb::engine_errc dcpControl(Cookie& cookie,
                           uint32_t opaque,
                           std::string_view key,
                           std::string_view val) {
    auto& connection = cookie.getConnection();
    auto* dcp = connection.getBucket().getDcpIface();
    auto ret = dcp->control(cookie, opaque, key, val);
    if (ret == cb::engine_errc::disconnect) {
        LOG_WARNING("{}: {} dcp.control returned cb::engine_errc::disconnect",
                    connection.getId(),
                    connection.getDescription());
        connection.setTerminationReason("Engine forced disconnect");
    }
    return ret;
}

cb::engine_errc dcpDeletion(Cookie& cookie,
                            uint32_t opaque,
                            const DocKey& key,
                            cb::const_byte_buffer value,
                            size_t privilegedPoolSize,
                            uint8_t datatype,
                            uint64_t cas,
                            Vbid vbid,
                            uint64_t bySeqno,
                            uint64_t revSeqno,
                            cb::const_byte_buffer meta) {
    auto& connection = cookie.getConnection();
    auto* dcp = connection.getBucket().getDcpIface();
    auto ret = dcp->deletion(cookie,
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
    if (ret == cb::engine_errc::disconnect) {
        LOG_WARNING("{}: {} dcp.deletion returned cb::engine_errc::disconnect",
                    connection.getId(),
                    connection.getDescription());
        connection.setTerminationReason("Engine forced disconnect");
    }
    return ret;
}

cb::engine_errc dcpDeletionV2(Cookie& cookie,
                              uint32_t opaque,
                              const DocKey& key,
                              cb::const_byte_buffer value,
                              size_t privilegedPoolSize,
                              uint8_t datatype,
                              uint64_t cas,
                              Vbid vbid,
                              uint64_t bySeqno,
                              uint64_t revSeqno,
                              uint32_t deleteTime) {
    auto& connection = cookie.getConnection();
    auto* dcp = connection.getBucket().getDcpIface();
    auto ret = dcp->deletion_v2(cookie,
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
    if (ret == cb::engine_errc::disconnect) {
        LOG_WARNING(
                "{}: {} dcp.deletion_v2 returned cb::engine_errc::disconnect",
                connection.getId(),
                connection.getDescription());
        connection.setTerminationReason("Engine forced disconnect");
    }
    return ret;
}

cb::engine_errc dcpExpiration(Cookie& cookie,
                              uint32_t opaque,
                              const DocKey& key,
                              cb::const_byte_buffer value,
                              size_t privilegedPoolSize,
                              uint8_t datatype,
                              uint64_t cas,
                              Vbid vbid,
                              uint64_t bySeqno,
                              uint64_t revSeqno,
                              uint32_t deleteTime) {
    auto& connection = cookie.getConnection();
    auto* dcp = connection.getBucket().getDcpIface();
    auto ret = dcp->expiration(cookie,
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
    if (ret == cb::engine_errc::disconnect) {
        LOG_WARNING(
                "{}: {} dcp.expiration returned cb::engine_errc::disconnect",
                connection.getId(),
                connection.getDescription());
        connection.setTerminationReason("Engine forced disconnect");
    }
    return ret;
}

cb::engine_errc dcpGetFailoverLog(Cookie& cookie,
                                  uint32_t opaque,
                                  Vbid vbucket,
                                  dcp_add_failover_log callback) {
    auto& connection = cookie.getConnection();
    auto* dcp = connection.getBucket().getDcpIface();
    auto ret = dcp->get_failover_log(cookie, opaque, vbucket, callback);
    if (ret == cb::engine_errc::disconnect) {
        LOG_WARNING(
                "{}: {} dcp.get_failover_log returned "
                "cb::engine_errc::disconnect",
                connection.getId(),
                connection.getDescription());
        connection.setTerminationReason("Engine forced disconnect");
    }
    return ret;
}

cb::engine_errc dcpMutation(Cookie& cookie,
                            uint32_t opaque,
                            const DocKey& key,
                            cb::const_byte_buffer value,
                            size_t privilegedPoolSize,
                            uint8_t datatype,
                            uint64_t cas,
                            Vbid vbid,
                            uint32_t flags,
                            uint64_t bySeqno,
                            uint64_t revSeqno,
                            uint32_t expiration,
                            uint32_t lockTime,
                            cb::const_byte_buffer meta,
                            uint8_t nru) {
    auto& connection = cookie.getConnection();
    auto* dcp = connection.getBucket().getDcpIface();
    auto ret = dcp->mutation(cookie,
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
    if (ret == cb::engine_errc::disconnect) {
        LOG_WARNING("{}: {} dcp.mutation returned cb::engine_errc::disconnect",
                    connection.getId(),
                    connection.getDescription());
        connection.setTerminationReason("Engine forced disconnect");
    }
    return ret;
}

cb::engine_errc dcpNoop(Cookie& cookie, uint32_t opaque) {
    auto& connection = cookie.getConnection();
    auto* dcp = connection.getBucket().getDcpIface();
    auto ret = dcp->noop(cookie, opaque);
    if (ret == cb::engine_errc::disconnect) {
        LOG_WARNING("{}: {} dcp.noop returned cb::engine_errc::disconnect",
                    connection.getId(),
                    connection.getDescription());
        connection.setTerminationReason("Engine forced disconnect");
    }
    return ret;
}

cb::engine_errc dcpOpen(Cookie& cookie,
                        uint32_t opaque,
                        uint32_t seqno,
                        uint32_t flags,
                        std::string_view name,
                        std::string_view value) {
    auto& connection = cookie.getConnection();
    auto* dcp = connection.getBucket().getDcpIface();
    auto ret = dcp->open(cookie, opaque, seqno, flags, name, value);
    if (ret == cb::engine_errc::disconnect) {
        LOG_WARNING("{}: {} dcp.open returned cb::engine_errc::disconnect",
                    connection.getId(),
                    connection.getDescription());
        connection.setTerminationReason("Engine forced disconnect");
    }
    return ret;
}

cb::engine_errc dcpSetVbucketState(Cookie& cookie,
                                   uint32_t opaque,
                                   Vbid vbid,
                                   vbucket_state_t state) {
    auto& connection = cookie.getConnection();
    auto* dcp = connection.getBucket().getDcpIface();
    auto ret = dcp->set_vbucket_state(cookie, opaque, vbid, state);
    if (ret == cb::engine_errc::disconnect) {
        LOG_WARNING(
                "{}: {} dcp.set_vbucket_state returned "
                "cb::engine_errc::disconnect",
                connection.getId(),
                connection.getDescription());
        connection.setTerminationReason("Engine forced disconnect");
    }
    return ret;
}

cb::engine_errc dcpSnapshotMarker(Cookie& cookie,
                                  uint32_t opaque,
                                  Vbid vbid,
                                  uint64_t startSeqno,
                                  uint64_t endSeqno,
                                  uint32_t flags,
                                  std::optional<uint64_t> highCompletedSeqno,
                                  std::optional<uint64_t> maxVisibleSeqno) {
    auto& connection = cookie.getConnection();
    auto* dcp = connection.getBucket().getDcpIface();
    auto ret = dcp->snapshot_marker(cookie,
                                    opaque,
                                    vbid,
                                    startSeqno,
                                    endSeqno,
                                    flags,
                                    highCompletedSeqno,
                                    maxVisibleSeqno);
    if (ret == cb::engine_errc::disconnect) {
        LOG_WARNING(
                "{}: {} dcp.snapshot_marker returned "
                "cb::engine_errc::disconnect",
                connection.getId(),
                connection.getDescription());
        connection.setTerminationReason("Engine forced disconnect");
    }
    return ret;
}

cb::engine_errc dcpStreamEnd(Cookie& cookie,
                             uint32_t opaque,
                             Vbid vbucket,
                             cb::mcbp::DcpStreamEndStatus status) {
    auto& connection = cookie.getConnection();
    auto* dcp = connection.getBucket().getDcpIface();
    auto ret = dcp->stream_end(cookie, opaque, vbucket, status);
    if (ret == cb::engine_errc::disconnect) {
        LOG_WARNING(
                "{}: {} dcp.stream_end returned cb::engine_errc::disconnect",
                connection.getId(),
                connection.getDescription());
        connection.setTerminationReason("Engine forced disconnect");
    }
    return ret;
}

cb::engine_errc dcpStreamReq(Cookie& cookie,
                             uint32_t flags,
                             uint32_t opaque,
                             Vbid vbucket,
                             uint64_t startSeqno,
                             uint64_t endSeqno,
                             uint64_t vbucketUuid,
                             uint64_t snapStartSeqno,
                             uint64_t snapEndSeqno,
                             uint64_t* rollbackSeqno,
                             dcp_add_failover_log callback,
                             std::optional<std::string_view> json) {
    auto& connection = cookie.getConnection();
    auto* dcp = connection.getBucket().getDcpIface();
    auto ret = dcp->stream_req(cookie,
                               flags,
                               opaque,
                               vbucket,
                               startSeqno,
                               endSeqno,
                               vbucketUuid,
                               snapStartSeqno,
                               snapEndSeqno,
                               rollbackSeqno,
                               callback,
                               json);
    if (ret == cb::engine_errc::disconnect) {
        LOG_WARNING(
                "{}: {} dcp.stream_req returned cb::engine_errc::disconnect",
                connection.getId(),
                connection.getDescription());
        connection.setTerminationReason("Engine forced disconnect");
    }
    return ret;
}

cb::engine_errc dcpSystemEvent(Cookie& cookie,
                               uint32_t opaque,
                               Vbid vbucket,
                               mcbp::systemevent::id eventId,
                               uint64_t bySeqno,
                               mcbp::systemevent::version version,
                               cb::const_byte_buffer key,
                               cb::const_byte_buffer eventData) {
    auto& connection = cookie.getConnection();
    auto* dcp = connection.getBucket().getDcpIface();
    auto ret = dcp->system_event(
            cookie, opaque, vbucket, eventId, bySeqno, version, key, eventData);
    if (ret == cb::engine_errc::disconnect) {
        LOG_WARNING(
                "{}: {} dcp.system_event returned cb::engine_errc::disconnect",
                connection.getId(),
                connection.getDescription());
        connection.setTerminationReason("Engine forced disconnect");
    }
    return ret;
}

cb::engine_errc dcpPrepare(Cookie& cookie,
                           uint32_t opaque,
                           const DocKey& key,
                           cb::const_byte_buffer value,
                           size_t priv_bytes,
                           uint8_t datatype,
                           uint64_t cas,
                           Vbid vbucket,
                           uint32_t flags,
                           uint64_t by_seqno,
                           uint64_t rev_seqno,
                           uint32_t expiration,
                           uint32_t lock_time,
                           uint8_t nru,
                           DocumentState document_state,
                           cb::durability::Level level) {
    auto& connection = cookie.getConnection();
    auto* dcp = connection.getBucket().getDcpIface();
    auto ret = dcp->prepare(cookie,
                            opaque,
                            key,
                            value,
                            priv_bytes,
                            datatype,
                            cas,
                            vbucket,
                            flags,
                            by_seqno,
                            rev_seqno,
                            expiration,
                            lock_time,
                            nru,
                            document_state,
                            level);
    if (ret == cb::engine_errc::disconnect) {
        LOG_WARNING("{}: {} dcp.prepare returned cb::engine_errc::disconnect",
                    connection.getId(),
                    connection.getDescription());
        connection.setTerminationReason("Engine forced disconnect");
    }
    return ret;
}

cb::engine_errc dcpSeqnoAcknowledged(Cookie& cookie,
                                     uint32_t opaque,
                                     Vbid vbucket,
                                     uint64_t prepared_seqno) {
    auto& connection = cookie.getConnection();
    auto* dcp = connection.getBucket().getDcpIface();
    auto ret = dcp->seqno_acknowledged(cookie, opaque, vbucket, prepared_seqno);
    if (ret == cb::engine_errc::disconnect) {
        LOG_WARNING(
                "{}: {} dcp.seqno_acknowledged returned "
                "cb::engine_errc::disconnect",
                connection.getId(),
                connection.getDescription());
        connection.setTerminationReason("Engine forced disconnect");
    }
    return ret;
}

cb::engine_errc dcpCommit(Cookie& cookie,
                          uint32_t opaque,
                          Vbid vbucket,
                          const DocKey& key,
                          uint64_t prepared_seqno,
                          uint64_t commit_seqno) {
    auto& connection = cookie.getConnection();
    auto* dcp = connection.getBucket().getDcpIface();
    auto ret = dcp->commit(
            cookie, opaque, vbucket, key, prepared_seqno, commit_seqno);
    if (ret == cb::engine_errc::disconnect) {
        LOG_WARNING("{}: {} dcp.commit returned cb::engine_errc::disconnect",
                    connection.getId(),
                    connection.getDescription());
        connection.setTerminationReason("Engine forced disconnect");
    }
    return ret;
}

cb::engine_errc dcpAbort(Cookie& cookie,
                         uint32_t opaque,
                         Vbid vbucket,
                         const DocKey& key,
                         uint64_t prepared_seqno,
                         uint64_t abort_seqno) {
    auto& connection = cookie.getConnection();
    auto* dcp = connection.getBucket().getDcpIface();
    auto ret = dcp->abort(
            cookie, opaque, vbucket, key, prepared_seqno, abort_seqno);
    if (ret == cb::engine_errc::disconnect) {
        LOG_WARNING("{}: {} dcp.abort returned cb::engine_errc::disconnect",
                    connection.getId(),
                    connection.getDescription());
        connection.setTerminationReason("Engine forced disconnect");
    }
    return ret;
}

std::pair<cb::engine_errc, cb::rangescan::Id> createRangeScan(
        Cookie& cookie,
        Vbid vbid,
        CollectionID cid,
        cb::rangescan::KeyView start,
        cb::rangescan::KeyView end,
        cb::rangescan::KeyOnly keyOnly,
        std::optional<cb::rangescan::SnapshotRequirements> snapshotReqs,
        std::optional<cb::rangescan::SamplingConfiguration> samplingConfig) {
    auto& c = cookie.getConnection();
    auto ret = c.getBucketEngine().createRangeScan(cookie,
                                                   vbid,
                                                   cid,
                                                   start,
                                                   end,
                                                   keyOnly,
                                                   snapshotReqs,
                                                   samplingConfig);

    if (ret.first == cb::engine_errc::disconnect) {
        LOG_WARNING("{}: {} createRangeScan return cb::engine_errc::disconnect",
                    c.getId(),
                    c.getDescription());
        c.setTerminationReason("Engine forced disconnect");
    }
    return ret;
}

cb::engine_errc continueRangeScan(Cookie& cookie,
                                  Vbid vbid,
                                  cb::rangescan::Id uuid,
                                  size_t itemLimit,
                                  std::chrono::milliseconds timeLimit) {
    auto& c = cookie.getConnection();
    auto ret = c.getBucketEngine().continueRangeScan(
            cookie, vbid, uuid, itemLimit, timeLimit);

    if (ret == cb::engine_errc::disconnect) {
        LOG_WARNING(
                "{}: {} continueRangeScan return cb::engine_errc::disconnect",
                c.getId(),
                c.getDescription());
        c.setTerminationReason("Engine forced disconnect");
    }
    return ret;
}

cb::engine_errc cancelRangeScan(Cookie& cookie,
                                Vbid vbid,
                                cb::rangescan::Id uuid) {
    auto& c = cookie.getConnection();
    auto ret = c.getBucketEngine().cancelRangeScan(cookie, vbid, uuid);

    if (ret == cb::engine_errc::disconnect) {
        LOG_WARNING("{}: {} cancelRangeScan return cb::engine_errc::disconnect",
                    c.getId(),
                    c.getDescription());
        c.setTerminationReason("Engine forced disconnect");
    }
    return ret;
}

cb::engine_errc bucket_set_parameter(Cookie& cookie,
                                     EngineParamCategory category,
                                     std::string_view key,
                                     std::string_view value,
                                     Vbid vbucket) {
    auto& connection = cookie.getConnection();
    auto ret = connection.getBucket().getEngine().setParameter(
            cookie, category, key, value, vbucket);
    if (ret == cb::engine_errc::disconnect) {
        LOG_WARNING(
                "{}: {} setParameter() returned cb::engine_errc::disconnect",
                connection.getId(),
                connection.getDescription());
        connection.setTerminationReason("Engine forced disconnect");
    }
    return ret;
}

cb::engine_errc bucket_compact_database(Cookie& cookie) {
    auto& connection = cookie.getConnection();

    const auto& req = cookie.getRequest();
    const auto& payload =
            req.getCommandSpecifics<cb::mcbp::request::CompactDbPayload>();

    auto ret = connection.getBucket().getEngine().compactDatabase(
            cookie,
            req.getVBucket(),
            payload.getPurgeBeforeTs(),
            payload.getPurgeBeforeSeq(),
            payload.getDropDeletes() != 0);
    if (ret == cb::engine_errc::disconnect) {
        LOG_WARNING(
                "{}: {} compactDatabase() returned cb::engine_errc::disconnect",
                connection.getId(),
                connection.getDescription());
        connection.setTerminationReason("Engine forced disconnect");
    }
    return ret;
}

std::pair<cb::engine_errc, vbucket_state_t> bucket_get_vbucket(Cookie& cookie) {
    auto& connection = cookie.getConnection();
    const auto& req = cookie.getRequest();
    auto ret = connection.getBucket().getEngine().getVBucket(cookie,
                                                             req.getVBucket());
    if (ret.first == cb::engine_errc::disconnect) {
        LOG_WARNING("{}: {} getVBucket() returned cb::engine_errc::disconnect",
                    connection.getId(),
                    connection.getDescription());
        connection.setTerminationReason("Engine forced disconnect");
    }

    return ret;
}

cb::engine_errc bucket_set_vbucket(Cookie& cookie,
                                   vbucket_state_t state,
                                   nlohmann::json& meta) {
    const auto& req = cookie.getRequest();
    auto& connection = cookie.getConnection();
    auto ret = connection.getBucket().getEngine().setVBucket(
            cookie,
            req.getVBucket(),
            req.getCas(),
            state,
            meta.empty() ? nullptr : &meta);
    if (ret == cb::engine_errc::disconnect) {
        LOG_WARNING("{}: {} setVBucket() returned cb::engine_errc::disconnect",
                    connection.getId(),
                    connection.getDescription());
        connection.setTerminationReason("Engine forced disconnect");
    }

    return ret;
}

cb::engine_errc bucket_delete_vbucket(Cookie& cookie, Vbid vbid, bool sync) {
    auto& connection = cookie.getConnection();
    auto ret = connection.getBucket().getEngine().deleteVBucket(
            cookie, vbid, sync);
    if (ret == cb::engine_errc::disconnect) {
        LOG_WARNING(
                "{}: {} deleteVBucket() returned cb::engine_errc::disconnect",
                connection.getId(),
                connection.getDescription());
        connection.setTerminationReason("Engine forced disconnect");
    }

    return ret;
}
