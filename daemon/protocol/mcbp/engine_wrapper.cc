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
#include <utility>

using namespace std::string_literals;

cb::engine_errc bucket_unknown_command(Cookie& cookie,
                                       const AddResponseFn& response) {
    auto& c = cookie.getConnection();
    auto ret = c.getBucketEngine().unknown_command(
            cookie, cookie.getRequest(), response);
    if (ret == cb::engine_errc::disconnect) {
        const auto request = cookie.getRequest();
        LOG_WARNING("{}: {} {} return cb::engine_errc::disconnect",
                    c.getId(),
                    c.getDescription().dump(),
                    to_string(request.getClientOpcode()));
        c.setTerminationReason("Engine forced disconnect");
    }
    return ret;
}

void bucket_reset_stats(Cookie& cookie) {
    auto& c = cookie.getConnection();
    c.getBucketEngine().reset_stats(cookie);
}

bool bucket_get_item_info(Connection& c,
                          const ItemIface& item_,
                          item_info& item_info_) {
    auto ret = c.getBucketEngine().get_item_info(item_, item_info_);
    LOG_TRACE("bucket_get_item_info() item:{} -> {}", item_, ret);
    if (!ret) {
        LOG_INFO("{}: {} bucket_get_item_info failed",
                 c.getId(),
                 c.getDescription().dump());
    }

    return ret;
}

cb::EngineErrorMetadataPair bucket_get_meta(Cookie& cookie,
                                            const DocKeyView& key,
                                            Vbid vbucket) {
    auto& c = cookie.getConnection();
    auto ret = c.getBucketEngine().get_meta(cookie, key, vbucket);
    if (ret.first == cb::engine_errc::success) {
        // For some reason nbytes and the key is not populated in
        // the item_info. For now just add the size of the key
        cookie.addDocumentReadBytes(key.size());
    }
    if (ret.first == cb::engine_errc::disconnect) {
        LOG_WARNING("{}: {} bucket_get_meta return cb::engine_errc::disconnect",
                    c.getId(),
                    c.getDescription().dump());
        c.setTerminationReason("Engine forced disconnect");
    }

    return ret;
}

cb::engine_errc bucket_store(
        Cookie& cookie,
        ItemIface& item_,
        uint64_t& cas,
        StoreSemantics operation,
        const std::optional<cb::durability::Requirements>& durability,
        DocumentState document_state,
        bool preserveTtl) {
    auto& c = cookie.getConnection();
    auto ret = c.getBucketEngine().store(cookie,
                                         item_,
                                         cas,
                                         operation,
                                         durability,
                                         document_state,
                                         preserveTtl);

    LOG_TRACE(
            "bucket_store() item:{} cas:{} op:{} durability:{} doc_state:{} -> "
            "{}",
            item_,
            cas,
            operation,
            (durability ? to_string(*durability) : "--"),
            document_state,
            ret);

    if (ret == cb::engine_errc::success) {
        using namespace cb::audit::document;
        add(cookie,
            document_state == DocumentState::Alive ? Operation::Modify
                                                   : Operation::Delete,
            cookie.getRequestKey());
        cookie.addDocumentWriteBytes(item_.getValueView().size() +
                                     item_.getDocKey().size());
    } else if (ret == cb::engine_errc::disconnect) {
        LOG_WARNING("{}: {} bucket_store return cb::engine_errc::disconnect",
                    c.getId(),
                    c.getDescription().dump());
        c.setTerminationReason("Engine forced disconnect");
    }

    return ret;
}

cb::EngineErrorCasPair bucket_store_if(
        Cookie& cookie,
        ItemIface& item_,
        uint64_t cas,
        StoreSemantics operation,
        const cb::StoreIfPredicate& predicate,
        const std::optional<cb::durability::Requirements>& durability,
        DocumentState document_state,
        bool preserveTtl) {
    auto& c = cookie.getConnection();
    auto [rstatus, rcas] = c.getBucketEngine().store_if(cookie,
                                                        item_,
                                                        cas,
                                                        operation,
                                                        predicate,
                                                        durability,
                                                        document_state,
                                                        preserveTtl);
    LOG_TRACE(
            "bucket_store_if() key:{} cas:{} operation:{} predicate:{} "
            "durability:{} doc_state:{} preserve_ttl:{} -> {}",
            item_.getDocKey().toPrintableString(),
            cas,
            operation,
            predicate ? "yes" : "no",
            durability.has_value() ? nlohmann::json(durability.value()).dump()
                                   : "no",
            document_state,
            preserveTtl ? "yes" : "no",
            rstatus);

    if (rstatus == cb::engine_errc::success) {
        using namespace cb::audit::document;
        add(cookie,
            document_state == DocumentState::Alive ? Operation::Modify
                                                   : Operation::Delete,
            cookie.getRequestKey());
        cookie.addDocumentWriteBytes(item_.getValueView().size() +
                                     item_.getDocKey().size());
    } else if (rstatus == cb::engine_errc::disconnect) {
        LOG_WARNING("{}: {} store_if return cb::engine_errc::disconnect",
                    c.getId(),
                    c.getDescription().dump());
        c.setTerminationReason("Engine forced disconnect");
    }

    return {rstatus, rcas};
}

cb::engine_errc bucket_remove(
        Cookie& cookie,
        const DocKeyView& key,
        uint64_t& cas,
        Vbid vbucket,
        const std::optional<cb::durability::Requirements>& durability,
        mutation_descr_t& mut_info) {
    auto& c = cookie.getConnection();
    auto ret = c.getBucketEngine().remove(
            cookie, key, cas, vbucket, durability, mut_info);
    if (ret == cb::engine_errc::success) {
        cb::audit::document::add(
                cookie, cb::audit::document::Operation::Delete, key);
        // @todo it should be 1 WCU for the tombstone?
        cookie.addDocumentWriteBytes(1);
    } else if (ret == cb::engine_errc::disconnect) {
        LOG_WARNING("{}: {} bucket_remove return cb::engine_errc::disconnect",
                    c.getId(),
                    c.getDescription().dump());
        c.setTerminationReason("Engine forced disconnect");
    }
    return ret;
}

cb::EngineErrorItemPair bucket_get(Cookie& cookie,
                                   const DocKeyView& key,
                                   Vbid vbucket,
                                   DocStateFilter documentStateFilter) {
    auto& c = cookie.getConnection();
    auto ret =
            c.getBucketEngine().get(cookie, key, vbucket, documentStateFilter);
    if (ret.first == cb::engine_errc::success) {
        cookie.addDocumentReadBytes(ret.second->getValueView().size() +
                                    ret.second->getDocKey().size());
    } else if (ret.first == cb::engine_errc::disconnect) {
        LOG_WARNING("{}: {} bucket_get return cb::engine_errc::disconnect",
                    c.getId(),
                    c.getDescription().dump());
        c.setTerminationReason("Engine forced disconnect");
    }
    LOG_TRACE("bucket_get() key:{} vbucket:{} docStateFilter:{} -> {}",
              cb::UserDataView(std::string_view{key}),
              vbucket,
              documentStateFilter,
              ret.first);

    return ret;
}

cb::EngineErrorItemPair bucket_get_replica(
        Cookie& cookie,
        const DocKeyView& key,
        Vbid vbucket,
        const DocStateFilter documentStateFilter) {
    auto& c = cookie.getConnection();
    auto ret = c.getBucketEngine().get_replica(
            cookie, key, vbucket, documentStateFilter);
    if (ret.first == cb::engine_errc::success) {
        cookie.addDocumentReadBytes(ret.second->getValueView().size() +
                                    ret.second->getDocKey().size());
    } else if (ret.first == cb::engine_errc::disconnect) {
        LOG_WARNING(
                "{}: {} bucket_get_replica return cb::engine_errc::disconnect",
                c.getId(),
                c.getDescription().dump());
        c.setTerminationReason("Engine forced disconnect");
    }
    LOG_TRACE("bucket_get_replica() key:{} vbucket:{} -> {}",
              cb::UserDataView(std::string_view{key}),
              vbucket,
              ret.first);

    return ret;
}

cb::EngineErrorItemPair bucket_get_random_document(Cookie& cookie,
                                                   CollectionID cid) {
    auto& c = cookie.getConnection();
    auto ret = c.getBucketEngine().get_random_document(cookie, cid);
    if (ret.first == cb::engine_errc::success) {
        cookie.addDocumentReadBytes(ret.second->getValueView().size() +
                                    ret.second->getDocKey().size());
    } else if (ret.first == cb::engine_errc::disconnect) {
        LOG_WARNING(
                "{}: {} bucket_get_random_document return "
                "cb::engine_errc::disconnect",
                c.getId(),
                c.getDescription().dump());
        c.setTerminationReason("Engine forced disconnect");
    }
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
        const DocKeyView& key,
        Vbid vbucket,
        const std::function<bool(const item_info&)>& filter) {
    auto& c = cookie.getConnection();
    auto ret = c.getBucketEngine().get_if(cookie, key, vbucket, filter);

    if (ret.first == cb::engine_errc::success && ret.second) {
        cookie.addDocumentReadBytes(ret.second->getValueView().size() +
                                    ret.second->getDocKey().size());
    } else if (ret.first == cb::engine_errc::disconnect) {
        LOG_WARNING("{}: {} bucket_get_if return cb::engine_errc::disconnect",
                    c.getId(),
                    c.getDescription().dump());
        c.setTerminationReason("Engine forced disconnect");
    }
    return ret;
}

cb::EngineErrorItemPair bucket_get_and_touch(
        Cookie& cookie,
        const DocKeyView& key,
        Vbid vbucket,
        uint32_t expiration,
        const std::optional<cb::durability::Requirements>& durability) {
    auto& c = cookie.getConnection();
    auto ret = c.getBucketEngine().get_and_touch(
            cookie, key, vbucket, expiration, durability);

    if (ret.first == cb::engine_errc::success) {
        cookie.addDocumentReadBytes(ret.second->getValueView().size() +
                                    ret.second->getDocKey().size());
        cookie.addDocumentWriteBytes(ret.second->getValueView().size() +
                                     ret.second->getDocKey().size());
    } else if (ret.first == cb::engine_errc::disconnect) {
        LOG_WARNING(
                "{}: {} bucket_get_and_touch return "
                "cb::engine_errc::disconnect",
                c.getId(),
                c.getDescription().dump());
        c.setTerminationReason("Engine forced disconnect");
    }
    return ret;
}

cb::EngineErrorItemPair bucket_get_locked(Cookie& cookie,
                                          const DocKeyView& key,
                                          Vbid vbucket,
                                          std::chrono::seconds lock_timeout) {
    auto& c = cookie.getConnection();
    auto ret =
            c.getBucketEngine().get_locked(cookie, key, vbucket, lock_timeout);

    if (ret.first == cb::engine_errc::success) {
        cb::audit::document::add(
                cookie, cb::audit::document::Operation::Lock, key);
        cookie.addDocumentReadBytes(ret.second->getValueView().size() +
                                    ret.second->getDocKey().size());
    } else if (ret.first == cb::engine_errc::disconnect) {
        LOG_WARNING(
                "{}: {} bucket_get_locked return cb::engine_errc::disconnect",
                c.getId(),
                c.getDescription().dump());
        c.setTerminationReason("Engine forced disconnect");
    }
    return ret;
}

size_t bucket_get_max_item_size(Cookie& cookie) {
    auto& c = cookie.getConnection();
    return c.getBucketEngine().getMaxItemSize();
}

cb::engine_errc bucket_unlock(Cookie& cookie,
                              const DocKeyView& key,
                              Vbid vbucket,
                              uint64_t cas) {
    auto& c = cookie.getConnection();
    auto ret = c.getBucketEngine().unlock(cookie, key, vbucket, cas);
    if (ret == cb::engine_errc::disconnect) {
        LOG_WARNING("{}: {} bucket_unlock return cb::engine_errc::disconnect",
                    c.getId(),
                    c.getDescription().dump());
        c.setTerminationReason("Engine forced disconnect");
    }
    return ret;
}

cb::unique_item_ptr bucket_allocate(Cookie& cookie,
                                    const DocKeyView& key,
                                    const size_t nbytes,
                                    const size_t priv_nbytes,
                                    const uint32_t flags,
                                    const rel_time_t exptime,
                                    uint8_t datatype,
                                    Vbid vbucket) {
    // MB-25650 - We've got a document of 0 byte value and claims to contain
    //            xattrs... that's not possible.
    if (nbytes == 0 && !cb::mcbp::datatype::is_raw(datatype)) {
        throw cb::engine_error(cb::engine_errc::invalid_arguments,
                               "bucket_allocate: Can't set datatype to " +
                                       cb::mcbp::datatype::to_string(datatype) +
                                       " for a 0 sized body");
    }

    if (priv_nbytes > cb::limits::PrivilegedBytes) {
        throw cb::engine_error(
                cb::engine_errc::too_big,
                "bucket_allocate: privileged bytes " +
                        std::to_string(priv_nbytes) + " exeeds max limit of " +
                        std::to_string(cb::limits::PrivilegedBytes));
    }

    auto& c = cookie.getConnection();
    try {
        LOG_TRACE(
                "bucket_allocate() key:{} nbytes:{} flags:{} exptime:{} "
                "datatype:{} vbucket:{}",
                cb::UserDataView(std::string_view(key)),
                nbytes,
                flags,
                exptime,
                datatype,
                vbucket);

        auto it = c.getBucketEngine().allocateItem(cookie,
                                                   key,
                                                   nbytes,
                                                   priv_nbytes,
                                                   flags,
                                                   exptime,
                                                   datatype,
                                                   vbucket);
        if (!it) {
            throw cb::engine_error(cb::engine_errc::no_memory,
                                   "bucket_allocate: engine returned no item");
        }
        return it;
    } catch (const cb::engine_error& err) {
        if (err.code() == cb::engine_errc::disconnect) {
            LOG_WARNING(
                    "{}: {} bucket_allocate return cb::engine_errc::disconnect",
                    c.getId(),
                    c.getDescription().dump());
            c.setTerminationReason("Engine forced disconnect");
        }
        throw;
    }
}

cb::engine_errc bucket_flush(Cookie& cookie) {
    auto& c = cookie.getConnection();
    auto ret = c.getBucketEngine().flush(cookie);
    if (ret == cb::engine_errc::disconnect) {
        LOG_WARNING("{}: {} bucket_flush return cb::engine_errc::disconnect",
                    c.getId(),
                    c.getDescription().dump());
        c.setTerminationReason("Engine forced disconnect");
    }
    return ret;
}

cb::engine_errc bucket_get_stats(Cookie& cookie,
                                 std::string_view key,
                                 std::string_view value,
                                 const AddStatFn& add_stat,
                                 const CheckYieldFn& check_yield) {
    auto& c = cookie.getConnection();
    auto ret = c.getBucketEngine().get_stats(
            cookie, key, value, add_stat, check_yield);
    if (ret == cb::engine_errc::disconnect) {
        LOG_WARNING(
                "{}: {} bucket_get_stats return cb::engine_errc::disconnect",
                c.getId(),
                c.getDescription().dump());
        c.setTerminationReason("Engine forced disconnect");
    }
    return ret;
}

cb::engine_errc bucket_start_persistence(Cookie& cookie) {
    auto& c = cookie.getConnection();
    auto ret = c.getBucketEngine().start_persistence(cookie);
    if (ret == cb::engine_errc::disconnect) {
        LOG_WARNING(
                "{}: {} bucket_start_persistence return "
                "cb::engine_errc::disconnect",
                c.getId(),
                c.getDescription().dump());
        c.setTerminationReason("Engine forced disconnect");
    }
    return ret;
}

cb::engine_errc bucket_stop_persistence(Cookie& cookie) {
    auto& c = cookie.getConnection();
    auto ret = c.getBucketEngine().stop_persistence(cookie);
    if (ret == cb::engine_errc::disconnect) {
        LOG_WARNING(
                "{}: {} bucket_stop_persistence return "
                "cb::engine_errc::disconnect",
                c.getId(),
                c.getDescription().dump());
        c.setTerminationReason("Engine forced disconnect");
    }
    return ret;
}

cb::engine_errc bucket_set_traffic_control_mode(Cookie& cookie,
                                                TrafficControlMode mode) {
    auto& c = cookie.getConnection();
    auto ret = c.getBucketEngine().set_traffic_control_mode(cookie, mode);
    if (ret == cb::engine_errc::disconnect) {
        LOG_WARNING(
                "{}: {} bucket_set_traffic_control_mode returned "
                "cb::engine_errc::disconnect when setting the mode to {}",
                c.getId(),
                c.getDescription().dump(),
                mode);
        c.setTerminationReason("Engine forced disconnect");
    }
    return ret;
}

cb::engine_errc bucket_evict_key(Cookie& cookie,
                                 const DocKeyView& key,
                                 Vbid vbucket) {
    auto& c = cookie.getConnection();
    auto ret = c.getBucketEngine().evict_key(
            cookie, cookie.getRequestKey(), vbucket);
    if (ret == cb::engine_errc::disconnect) {
        LOG_WARNING(
                "{}: {} bucket_evict_key returned cb::engine_errc::disconnect",
                c.getId(),
                c.getDescription().dump());
        c.setTerminationReason("Engine forced disconnect");
    }
    return ret;
}

cb::engine_errc bucket_wait_for_seqno_persistence(Cookie& cookie,
                                                  uint64_t seqno,
                                                  Vbid vbid) {
    auto& c = cookie.getConnection();
    auto ret =
            c.getBucketEngine().wait_for_seqno_persistence(cookie, seqno, vbid);
    if (ret == cb::engine_errc::disconnect) {
        LOG_WARNING(
                "{}: {} bucket_wait_for_seqno_persistence returned "
                "cb::engine_errc::disconnect",
                c.getId(),
                c.getDescription().dump());
        c.setTerminationReason("Engine forced disconnect");
    }
    return ret;
}

cb::engine_errc bucket_observe(
        Cookie& cookie,
        const DocKeyView& key,
        Vbid vbucket,
        const std::function<void(uint8_t, uint64_t)>& key_handler,
        uint64_t& persist_time_hint) {
    auto& c = cookie.getConnection();
    auto ret = c.getBucketEngine().observe(
            cookie, key, vbucket, key_handler, persist_time_hint);
    if (ret == cb::engine_errc::disconnect) {
        LOG_WARNING(
                "{}: {} bucket_observe returned cb::engine_errc::disconnect",
                c.getId(),
                c.getDescription().dump());
        c.setTerminationReason("Engine forced disconnect");
    }
    return ret;
}

cb::engine_errc dcpAddStream(Cookie& cookie,
                             uint32_t opaque,
                             Vbid vbid,
                             cb::mcbp::DcpAddStreamFlag flags) {
    auto& connection = cookie.getConnection();
    auto* dcp = connection.getBucket().getDcpIface();
    auto ret = dcp->add_stream(cookie, opaque, vbid, flags);
    if (ret == cb::engine_errc::disconnect) {
        LOG_WARNING(
                "{}: {} dcp.add_stream returned cb::engine_errc::disconnect",
                connection.getId(),
                connection.getDescription().dump());
        connection.setTerminationReason("Engine forced disconnect");
    }
    return ret;
}

cb::engine_errc dcpBufferAcknowledgement(Cookie& cookie,
                                         uint32_t opaque,
                                         uint32_t ackSize) {
    auto& connection = cookie.getConnection();
    auto* dcp = connection.getBucket().getDcpIface();
    auto ret = dcp->buffer_acknowledgement(cookie, opaque, ackSize);
    if (ret == cb::engine_errc::disconnect) {
        LOG_WARNING(
                "{}: {} dcp.buffer_acknowledgement returned "
                "cb::engine_errc::disconnect",
                connection.getId(),
                connection.getDescription().dump());
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
                connection.getDescription().dump());
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
                    connection.getDescription().dump());
        connection.setTerminationReason("Engine forced disconnect");
    }
    return ret;
}

cb::engine_errc dcpDeletion(Cookie& cookie,
                            uint32_t opaque,
                            const DocKeyView& key,
                            cb::const_byte_buffer value,
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
                             datatype,
                             cas,
                             vbid,
                             bySeqno,
                             revSeqno,
                             meta);
    if (ret == cb::engine_errc::success && !connection.isInternal()) {
        cookie.addDocumentWriteBytes(value.size() + key.size());
    } else if (ret == cb::engine_errc::disconnect) {
        LOG_WARNING("{}: {} dcp.deletion returned cb::engine_errc::disconnect",
                    connection.getId(),
                    connection.getDescription().dump());
        connection.setTerminationReason("Engine forced disconnect");
    }
    return ret;
}

cb::engine_errc dcpDeletionV2(Cookie& cookie,
                              uint32_t opaque,
                              const DocKeyView& key,
                              cb::const_byte_buffer value,
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
                                datatype,
                                cas,
                                vbid,
                                bySeqno,
                                revSeqno,
                                deleteTime);
    if (ret == cb::engine_errc::success && !connection.isInternal()) {
        cookie.addDocumentWriteBytes(value.size() + key.size());
    } else if (ret == cb::engine_errc::disconnect) {
        LOG_WARNING(
                "{}: {} dcp.deletion_v2 returned cb::engine_errc::disconnect",
                connection.getId(),
                connection.getDescription().dump());
        connection.setTerminationReason("Engine forced disconnect");
    }
    return ret;
}

cb::engine_errc dcpExpiration(Cookie& cookie,
                              uint32_t opaque,
                              const DocKeyView& key,
                              cb::const_byte_buffer value,
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
                               datatype,
                               cas,
                               vbid,
                               bySeqno,
                               revSeqno,
                               deleteTime);
    if (ret == cb::engine_errc::success && !connection.isInternal()) {
        cookie.addDocumentWriteBytes(value.size() + key.size());
    } else if (ret == cb::engine_errc::disconnect) {
        LOG_WARNING(
                "{}: {} dcp.expiration returned cb::engine_errc::disconnect",
                connection.getId(),
                connection.getDescription().dump());
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
                connection.getDescription().dump());
        connection.setTerminationReason("Engine forced disconnect");
    }
    return ret;
}

cb::engine_errc dcpMutation(Cookie& cookie,
                            uint32_t opaque,
                            const DocKeyView& key,
                            cb::const_byte_buffer value,
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
    if (ret == cb::engine_errc::success && !connection.isInternal()) {
        cookie.addDocumentWriteBytes(value.size() + key.size());
    } else if (ret == cb::engine_errc::disconnect) {
        LOG_WARNING("{}: {} dcp.mutation returned cb::engine_errc::disconnect",
                    connection.getId(),
                    connection.getDescription().dump());
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
                    connection.getDescription().dump());
        connection.setTerminationReason("Engine forced disconnect");
    }
    return ret;
}

cb::engine_errc dcpOpen(Cookie& cookie,
                        uint32_t opaque,
                        uint32_t seqno,
                        cb::mcbp::DcpOpenFlag flags,
                        std::string_view name,
                        std::string_view value) {
    auto& connection = cookie.getConnection();
    auto* dcp = connection.getBucket().getDcpIface();
    auto ret = dcp->open(cookie, opaque, seqno, flags, name, value);
    if (ret == cb::engine_errc::disconnect) {
        LOG_WARNING("{}: {} dcp.open returned cb::engine_errc::disconnect",
                    connection.getId(),
                    connection.getDescription().dump());
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
                connection.getDescription().dump());
        connection.setTerminationReason("Engine forced disconnect");
    }
    return ret;
}

cb::engine_errc dcpSnapshotMarker(
        Cookie& cookie,
        uint32_t opaque,
        Vbid vbid,
        uint64_t startSeqno,
        uint64_t endSeqno,
        cb::mcbp::request::DcpSnapshotMarkerFlag flags,
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
                connection.getDescription().dump());
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
                connection.getDescription().dump());
        connection.setTerminationReason("Engine forced disconnect");
    }
    return ret;
}

cb::engine_errc dcpStreamReq(Cookie& cookie,
                             cb::mcbp::DcpAddStreamFlag flags,
                             uint32_t opaque,
                             Vbid vbucket,
                             uint64_t startSeqno,
                             uint64_t endSeqno,
                             uint64_t vbucketUuid,
                             uint64_t snapStartSeqno,
                             uint64_t snapEndSeqno,
                             uint64_t purgeSeqno,
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
                               purgeSeqno,
                               rollbackSeqno,
                               std::move(callback),
                               json);
    if (ret == cb::engine_errc::disconnect) {
        LOG_WARNING(
                "{}: {} dcp.stream_req returned cb::engine_errc::disconnect",
                connection.getId(),
                connection.getDescription().dump());
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
                connection.getDescription().dump());
        connection.setTerminationReason("Engine forced disconnect");
    }
    return ret;
}

cb::engine_errc dcpPrepare(Cookie& cookie,
                           uint32_t opaque,
                           const DocKeyView& key,
                           cb::const_byte_buffer value,
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
                    connection.getDescription().dump());
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
                connection.getDescription().dump());
        connection.setTerminationReason("Engine forced disconnect");
    }
    return ret;
}

cb::engine_errc dcpCommit(Cookie& cookie,
                          uint32_t opaque,
                          Vbid vbucket,
                          const DocKeyView& key,
                          uint64_t prepared_seqno,
                          uint64_t commit_seqno) {
    auto& connection = cookie.getConnection();
    auto* dcp = connection.getBucket().getDcpIface();
    auto ret = dcp->commit(
            cookie, opaque, vbucket, key, prepared_seqno, commit_seqno);
    if (ret == cb::engine_errc::disconnect) {
        LOG_WARNING("{}: {} dcp.commit returned cb::engine_errc::disconnect",
                    connection.getId(),
                    connection.getDescription().dump());
        connection.setTerminationReason("Engine forced disconnect");
    }
    return ret;
}

cb::engine_errc dcpAbort(Cookie& cookie,
                         uint32_t opaque,
                         Vbid vbucket,
                         const DocKeyView& key,
                         uint64_t prepared_seqno,
                         uint64_t abort_seqno) {
    auto& connection = cookie.getConnection();
    auto* dcp = connection.getBucket().getDcpIface();
    auto ret = dcp->abort(
            cookie, opaque, vbucket, key, prepared_seqno, abort_seqno);
    if (ret == cb::engine_errc::disconnect) {
        LOG_WARNING("{}: {} dcp.abort returned cb::engine_errc::disconnect",
                    connection.getId(),
                    connection.getDescription().dump());
        connection.setTerminationReason("Engine forced disconnect");
    }
    return ret;
}

std::pair<cb::engine_errc, cb::rangescan::Id> createRangeScan(
        Cookie& cookie, const cb::rangescan::CreateParameters& params) {
    auto& c = cookie.getConnection();
    auto ret = c.getBucketEngine().createRangeScan(cookie, params);

    if (ret.first == cb::engine_errc::disconnect) {
        LOG_WARNING("{}: {} createRangeScan return cb::engine_errc::disconnect",
                    c.getId(),
                    c.getDescription().dump());
        c.setTerminationReason("Engine forced disconnect");
    }
    return ret;
}

cb::engine_errc continueRangeScan(
        Cookie& cookie, const cb::rangescan::ContinueParameters& params) {
    auto& c = cookie.getConnection();
    auto ret = c.getBucketEngine().continueRangeScan(cookie, params);

    if (ret == cb::engine_errc::disconnect) {
        LOG_WARNING(
                "{}: {} continueRangeScan return cb::engine_errc::disconnect",
                c.getId(),
                c.getDescription().dump());
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
                    c.getDescription().dump());
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
    auto ret = connection.getBucketEngine().setParameter(
            cookie, category, key, value, vbucket);
    if (ret == cb::engine_errc::disconnect) {
        LOG_WARNING(
                "{}: {} setParameter() returned cb::engine_errc::disconnect",
                connection.getId(),
                connection.getDescription().dump());
        connection.setTerminationReason("Engine forced disconnect");
    }
    return ret;
}

cb::engine_errc bucket_compact_database(Cookie& cookie) {
    auto& connection = cookie.getConnection();

    const auto& req = cookie.getRequest();
    const auto& payload =
            req.getCommandSpecifics<cb::mcbp::request::CompactDbPayload>();

    std::vector<std::string> keys;
    if (!req.getValueString().empty()) {
        try {
            const auto json = nlohmann::json::parse(req.getValueString());
            if (!json.is_array()) {
                LOG_WARNING_CTX(
                        "bucket_compact_database: obsolete keys should be a "
                        "list of keys",
                        {"value", req.getValueString()});
                return cb::engine_errc::invalid_arguments;
            }
            keys = json;
        } catch (const std::exception& exception) {
            LOG_WARNING_CTX(
                    "bucket_compact_database: Failed to decode obsolete keys",
                    {"value", req.getValueString()});
            return cb::engine_errc::invalid_arguments;
        }
    }

    auto ret = connection.getBucketEngine().compactDatabase(
            cookie,
            req.getVBucket(),
            payload.getPurgeBeforeTs(),
            payload.getPurgeBeforeSeq(),
            payload.getDropDeletes() != 0,
            keys);
    if (ret == cb::engine_errc::disconnect) {
        LOG_WARNING(
                "{}: {} compactDatabase() returned cb::engine_errc::disconnect",
                connection.getId(),
                connection.getDescription().dump());
        connection.setTerminationReason("Engine forced disconnect");
    }
    return ret;
}

std::pair<cb::engine_errc, vbucket_state_t> bucket_get_vbucket(Cookie& cookie) {
    auto& connection = cookie.getConnection();
    const auto& req = cookie.getRequest();
    auto ret =
            connection.getBucketEngine().getVBucket(cookie, req.getVBucket());
    if (ret.first == cb::engine_errc::disconnect) {
        LOG_WARNING("{}: {} getVBucket() returned cb::engine_errc::disconnect",
                    connection.getId(),
                    connection.getDescription().dump());
        connection.setTerminationReason("Engine forced disconnect");
    }

    return ret;
}

cb::engine_errc bucket_set_vbucket(Cookie& cookie,
                                   vbucket_state_t state,
                                   nlohmann::json& meta) {
    const auto& req = cookie.getRequest();
    auto& connection = cookie.getConnection();
    auto ret = connection.getBucketEngine().setVBucket(
            cookie,
            req.getVBucket(),
            req.getCas(),
            state,
            meta.empty() ? nullptr : &meta);
    if (ret == cb::engine_errc::disconnect) {
        LOG_WARNING("{}: {} setVBucket() returned cb::engine_errc::disconnect",
                    connection.getId(),
                    connection.getDescription().dump());
        connection.setTerminationReason("Engine forced disconnect");
    }

    return ret;
}

cb::engine_errc bucket_delete_vbucket(Cookie& cookie, Vbid vbid, bool sync) {
    auto& connection = cookie.getConnection();
    auto ret = connection.getBucketEngine().deleteVBucket(cookie, vbid, sync);
    if (ret == cb::engine_errc::disconnect) {
        LOG_WARNING(
                "{}: {} deleteVBucket() returned cb::engine_errc::disconnect",
                connection.getId(),
                connection.getDescription().dump());
        connection.setTerminationReason("Engine forced disconnect");
    }

    return ret;
}
