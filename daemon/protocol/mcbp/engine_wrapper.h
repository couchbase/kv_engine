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
#pragma once

/**
 * This file contains wrapper functions on top of the engine interface.
 * If you want to know more information of a function or the arguments
 * it takes, you should look in `memcached/engine.h`.
 *
 * The cookie contains the connection with the engine to use, and the
 * wrapper methods performs accounting of metering data (when appropriate).
 *
 * If the underlying engine requests the connection to disconnect the
 * wrapper method should log it, and update the termination reason for
 * the cookie (as it gets injected in the audit event).
 *
 * Note: We're working on cleaning up this API from a C api to a C++ API, so
 * it is no longer consistent (some methods takes pointers, whereas others
 * take reference to objects). We might do a full scrub of the API at
 * some point.
 */

#include <daemon/connection.h>
#include <memcached/dcp.h>
#include <memcached/engine.h>
#include <memcached/engine_error.h>

namespace cb::mcbp {
enum class DcpOpenFlag : uint32_t;
}

cb::engine_errc bucket_unknown_command(Cookie& cookie,
                                       const AddResponseFn& response);

void bucket_reset_stats(Cookie& cookie);

bool bucket_get_item_info(Connection& c,
                          const ItemIface& item_,
                          item_info& item_info_);

cb::EngineErrorMetadataPair bucket_get_meta(Cookie& cookie,
                                            const DocKeyView& key,
                                            Vbid vbucket);

cb::engine_errc bucket_store(
        Cookie& cookie,
        ItemIface& item_,
        uint64_t& cas,
        StoreSemantics operation,
        const std::optional<cb::durability::Requirements>& durability,
        DocumentState document_state,
        bool preserveTtl);

cb::EngineErrorCasPair bucket_store_if(
        Cookie& cookie,
        ItemIface& item_,
        uint64_t cas,
        StoreSemantics operation,
        const cb::StoreIfPredicate& predicate,
        const std::optional<cb::durability::Requirements>& durability,
        DocumentState document_state,
        bool preserveTtl);

cb::engine_errc bucket_remove(
        Cookie& cookie,
        const DocKeyView& key,
        uint64_t& cas,
        Vbid vbucket,
        const std::optional<cb::durability::Requirements>& durability,
        mutation_descr_t& mut_info);

cb::EngineErrorItemPair bucket_get(
        Cookie& cookie,
        const DocKeyView& key,
        Vbid vbucket,
        DocStateFilter documentStateFilter = DocStateFilter::Alive);

cb::EngineErrorItemPair bucket_get_replica(Cookie& cookie,
                                           const DocKeyView& key,
                                           Vbid vbucket,
                                           DocStateFilter documentStateFilter);

cb::EngineErrorItemPair bucket_get_random_document(Cookie& cookie,
                                                   CollectionID cid);

cb::EngineErrorItemPair bucket_get_if(
        Cookie& cookie,
        const DocKeyView& key,
        Vbid vbucket,
        const std::function<bool(const item_info&)>& filter);

cb::EngineErrorItemPair bucket_get_and_touch(
        Cookie& cookie,
        const DocKeyView& key,
        Vbid vbucket,
        uint32_t expiration,
        const std::optional<cb::durability::Requirements>& durability);

BucketCompressionMode bucket_get_compression_mode(Cookie& cookie);

size_t bucket_get_max_item_size(Cookie& cookie);

float bucket_min_compression_ratio(Cookie& cookie);

cb::EngineErrorItemPair bucket_get_locked(Cookie& cookie,
                                          const DocKeyView& key,
                                          Vbid vbucket,
                                          std::chrono::seconds lock_timeout);

cb::engine_errc bucket_unlock(Cookie& cookie,
                              const DocKeyView& key,
                              Vbid vbucket,
                              uint64_t cas);

cb::unique_item_ptr bucket_allocate(Cookie& cookie,
                                    const DocKeyView& key,
                                    size_t nbytes,
                                    size_t priv_nbytes,
                                    uint32_t flags,
                                    rel_time_t exptime,
                                    uint8_t datatype,
                                    Vbid vbucket);

cb::engine_errc bucket_flush(Cookie& cookie);

cb::engine_errc bucket_get_stats(
        Cookie& cookie,
        std::string_view key,
        std::string_view value,
        const AddStatFn& add_stat,
        const CheckYieldFn& check_yield = []() { return false; });

cb::engine_errc bucket_start_persistence(Cookie& cookie);
cb::engine_errc bucket_stop_persistence(Cookie& cookie);
cb::engine_errc bucket_set_traffic_control_mode(Cookie& cookie,
                                                TrafficControlMode mode);
cb::engine_errc bucket_evict_key(Cookie& cookie,
                                 const DocKeyView& key,
                                 Vbid vbucket);
cb::engine_errc bucket_wait_for_seqno_persistence(Cookie& cookie,
                                                  uint64_t seqno,
                                                  Vbid vbid);
cb::engine_errc bucket_observe(
        Cookie& cookie,
        const DocKeyView& key,
        Vbid vbucket,
        const std::function<void(uint8_t, uint64_t)>& key_handler,
        uint64_t& persist_time_hint);

/**
 * Calls the underlying engine DCP add-stream
 *
 * @param cookie The cookie representing the connection
 * @param opaque The opaque field in the received message
 * @param vbid The vbucket id
 * @param flags
 * @return cb::engine_errc
 */
cb::engine_errc dcpAddStream(Cookie& cookie,
                             uint32_t opaque,
                             Vbid vbid,
                             cb::mcbp::DcpAddStreamFlag flags);

/**
 * Calls the underlying engine DCP buffer-acknowledgement
 *
 * @param cookie The cookie representing the connection
 * @param opaque The opaque field in the received message
 * @param ackSize The number of acknowledged bytes
 * @return cb::engine_errc
 */
cb::engine_errc dcpBufferAcknowledgement(Cookie& cookie,
                                         uint32_t opaque,
                                         uint32_t ackSize);

/**
 * Calls the underlying engine DCP close-stream
 *
 * @param cookie The cookie representing the connection
 * @param opaque The opaque field in the received message
 * @param vbid The vbucket id
 * @param sid The ID of the stream
 * @return cb::engine_errc
 */
cb::engine_errc dcpCloseStream(Cookie& cookie,
                               uint32_t opaque,
                               Vbid vbid,
                               cb::mcbp::DcpStreamId sid);

/**
 * Calls the underlying engine DCP control
 *
 * @param cookie The cookie representing the connection
 * @param opaque The opaque field in the received message
 * @param key The parameter to set
 * @param value The value to set
 * @return cb::engine_errc
 */
cb::engine_errc dcpControl(Cookie& cookie,
                           uint32_t opaque,
                           std::string_view key,
                           std::string_view val);

/**
 * Calls the underlying engine DCP deletion
 *
 * @param cookie The cookie representing the connection
 * @param opaque The opaque field in the received message
 * @param key The document key
 * @param value The document value to store
 * @param datatype The document datatype
 * @param cas The documents CAS
 * @param vbid The vbucket id
 * @param bySeqno The db sequence number
 * @param revSeqno The revision sequence number
 * @return cb::engine_errc
 */
cb::engine_errc dcpDeletion(Cookie& cookie,
                            uint32_t opaque,
                            const DocKeyView& key,
                            cb::const_byte_buffer value,
                            uint8_t datatype,
                            uint64_t cas,
                            Vbid vbid,
                            uint64_t bySeqno,
                            uint64_t revSeqno);

/**
 * Calls the underlying engine DCP deletion v2
 *
 * @param cookie The cookie representing the connection
 * @param opaque The opaque field in the received message
 * @param key The document key
 * @param value The document value to store
 * @param datatype The document datatype
 * @param cas The documents CAS
 * @param vbid The vbucket id
 * @param bySeqno The db sequence number
 * @param revSeqno The revision sequence number
 * @param deleteTime The time of the deletion
 * @return cb::engine_errc
 */
cb::engine_errc dcpDeletionV2(Cookie& cookie,
                              uint32_t opaque,
                              const DocKeyView& key,
                              cb::const_byte_buffer value,
                              uint8_t datatype,
                              uint64_t cas,
                              Vbid vbid,
                              uint64_t bySeqno,
                              uint64_t revSeqno,
                              uint32_t deleteTime);

/**
 * Calls the underlying engine DCP expiration
 *
 * @param cookie The cookie representing the connection
 * @param opaque The opaque field in the received message
 * @param key The document key
 * @param value The document value to store
 * @param datatype The document datatype
 * @param cas The documents CAS
 * @param vbid The vbucket id
 * @param bySeqno The db sequence number
 * @param revSeqno The revision sequence number
 * @param deleteTime The time of the deletion
 * @return cb::engine_errc
 */
cb::engine_errc dcpExpiration(Cookie& cookie,
                              uint32_t opaque,
                              const DocKeyView& key,
                              cb::const_byte_buffer value,
                              uint8_t datatype,
                              uint64_t cas,
                              Vbid vbid,
                              uint64_t bySeqno,
                              uint64_t revSeqno,
                              uint32_t deleteTime);

/**
 * Calls the underlying engine DCP get-failover-log
 *
 * @param cookie The cookie representing the connection
 * @param opaque The opaque field in the received message
 * @param vbid The vbucket id
 * @param callback The callback to be used by the engine to add the response
 * @return cb::engine_errc
 */
cb::engine_errc dcpGetFailoverLog(Cookie& cookie,
                                  uint32_t opaque,
                                  Vbid vbid,
                                  dcp_add_failover_log callback);

/**
 * Calls the underlying engine DCP mutation
 *
 * @param cookie The cookie representing the connection
 * @param opaque The opaque field in the received message
 * @param key The document key
 * @param value The document value to store
 * @param datatype The document datatype
 * @param cas The documents CAS
 * @param vbid The vbucket id
 * @param flags
 * @param bySeqno The db sequence number
 * @param revSeqno The revision sequence number
 * @param expiration The document expiration
 * @param lockTime The document lock time
 * @param nru The document NRU
 * @return cb::engine_errc
 */
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
                            uint8_t nru);

/**
 * Calls the underlying engine DCP cached_value
 *
 * @param cookie The cookie representing the connection
 * @param opaque The opaque field in the received message
 * @param key The document key
 * @param value The document value to store
 * @param datatype The document datatype
 * @param cas The documents CAS
 * @param vbid The vbucket id
 * @param flags
 * @param bySeqno The db sequence number
 * @param revSeqno The revision sequence number
 * @param expiration The document expiration
 * @param nru The document NRU
 * @return cb::engine_errc
 */
cb::engine_errc dcpCachedValue(Cookie& cookie,
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
                               uint8_t nru);

/**
 * Calls the underlying engine DCP cached_key_meta
 *
 * @param cookie The cookie representing the connection
 * @param opaque The opaque field in the received message
 * @param key The document key
 * @param datatype The document datatype
 * @param cas The documents CAS
 * @param vbid The vbucket id
 * @param flags
 * @param bySeqno The db sequence number
 * @param revSeqno The revision sequence number
 * @param expiration The document expiration
 * @return cb::engine_errc
 */
cb::engine_errc dcpCachedKeyMeta(Cookie& cookie,
                                 uint32_t opaque,
                                 const DocKeyView& key,
                                 uint8_t datatype,
                                 uint64_t cas,
                                 Vbid vbid,
                                 uint32_t flags,
                                 uint64_t bySeqno,
                                 uint64_t revSeqno,
                                 uint32_t expiration);

/**
 * Calls the underlying engine DCP cache_transfer_end
 *
 * @param cookie The cookie representing the connection
 * @param opaque The opaque field in the received message
 * @param vbid The vbucket id
 * @return cb::engine_errc
 */
cb::engine_errc dcpCacheTransferEnd(Cookie& cookie,
                                    uint32_t opaque,
                                    Vbid vbucket);

/**
 * Calls the underlying engine DCP noop
 *
 * @param cookie The cookie representing the connection
 * @param opaque The opaque field in the received message
 * @return cb::engine_errc
 */
cb::engine_errc dcpNoop(Cookie& cookie, uint32_t opaque);

/**
 * Calls the underlying engine DCP open
 *
 * @param cookie The cookie representing the connection
 * @param opaque The opaque field in the received message
 * @param seqno
 * @param flags The DCP open flags
 * @param name The DCP connection name
 * @return cb::engine_errc
 */
cb::engine_errc dcpOpen(Cookie& cookie,
                        uint32_t opaque,
                        uint32_t seqno,
                        cb::mcbp::DcpOpenFlag flags,
                        std::string_view name,
                        std::string_view value);

/**
 * Calls the underlying engine DCP set-vbucket-state
 *
 * @param cookie The cookie representing the connection
 * @param opaque The opaque field in the received message
 * @param vbid The vbucket id
 * @param state The new vbucket state
 * @return cb::engine_errc
 */
cb::engine_errc dcpSetVbucketState(Cookie& cookie,
                                   uint32_t opaque,
                                   Vbid vbid,
                                   vbucket_state_t state);

/**
 * Calls the underlying engine DCP snapshot-marker
 *
 * @param cookie The cookie representing the connection
 * @param opaque The opaque field in the received message
 * @param vbid The vbucket id
 * @param startSeqno The snapshot start seqno
 * @param endSeqno The snapshot end seqno
 * @param flags
 * @param highCompletedSeqno The SyncRepl high completed seqno
 * @param highPreparedSeqno The SyncRepl high prepared seqno
 * @param maxVisibleSeqno The snapshot's maximum visible seqno
 * @param purgeSeqno The snapshot's purge seqno
 *
 * @return cb::engine_errc
 */
cb::engine_errc dcpSnapshotMarker(
        Cookie& cookie,
        uint32_t opaque,
        Vbid vbid,
        uint64_t startSeqno,
        uint64_t endSeqno,
        cb::mcbp::request::DcpSnapshotMarkerFlag flags,
        std::optional<uint64_t> highCompletedSeqno,
        std::optional<uint64_t> highPreparedSeqno,
        std::optional<uint64_t> maxVisibleSeqno,
        std::optional<uint64_t> purgeSeqno);

/**
 * Calls the underlying engine DCP stream-end
 *
 * @param cookie The cookie representing the connection
 * @param opaque The opaque field in the received message
 * @param vbid The vbucket id
 * @param status The stream end status code
 * @return cb::engine_errc
 */
cb::engine_errc dcpStreamEnd(Cookie& cookie,
                             uint32_t opaque,
                             Vbid vbid,
                             cb::mcbp::DcpStreamEndStatus status);

/**
 * Calls the underlying engine DCP stream-req
 *
 * @param cookie The cookie representing the connection
 * @param flags
 * @param opaque The opaque field in the received message
 * @param vbid The vbucket id
 * @param startSeqno The start seqno
 * @param endSeqno The end seqno
 * @param vbucketUuid The vbucket UUID
 * @param snapshotStartSeqno The start seqno of the last received snapshot
 * @param snapshotEndSeqno The end seqno of the last received snapshot
 * @param rollbackSeqno Used by the engine to add the rollback seqno in the
 * response
 * @param callback The callback to be used by the engine to add the failover log
 * in the response
 * @param json Optional JSON string; which if non-empty can be used
 *                   to further control how data is requested - for example
 *                   to filter collections.
 * @return cb::engine_errc
 */
cb::engine_errc dcpStreamReq(Cookie& cookie,
                             cb::mcbp::DcpAddStreamFlag flags,
                             uint32_t opaque,
                             Vbid vbid,
                             uint64_t startSeqno,
                             uint64_t endSeqno,
                             uint64_t vbucketUuid,
                             uint64_t snapStartSeqno,
                             uint64_t snapEndSeqno,
                             uint64_t* rollbackSeqno,
                             dcp_add_failover_log callback,
                             std::optional<std::string_view> json);

/**
 * Calls the underlying engine DCP system-event
 *
 * @param cookie The cookie representing the connection
 * @param opaque The opaque field in the received message
 * @param vbid The vbucket id
 * @param eventId The event id
 * @param bySeqno
 * @param version A version value which defines the key and data of the event
 * @param eventKey The event key
 * @param eventData The event data
 * @return cb::engine_errc
 */
cb::engine_errc dcpSystemEvent(Cookie& cookie,
                               uint32_t opaque,
                               Vbid vbucket,
                               mcbp::systemevent::id eventId,
                               uint64_t bySeqno,
                               mcbp::systemevent::version version,
                               cb::const_byte_buffer eventKey,
                               cb::const_byte_buffer eventData);

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
                           cb::durability::Level level);

cb::engine_errc dcpSeqnoAcknowledged(Cookie& cookie,
                                     uint32_t opaque,
                                     Vbid vbucket,
                                     uint64_t prepared_seqno);

cb::engine_errc dcpCommit(Cookie& cookie,
                          uint32_t opaque,
                          Vbid vbucket,
                          const DocKeyView& key,
                          uint64_t prepared_seqno,
                          uint64_t commit_seqno);

cb::engine_errc dcpAbort(Cookie& cookie,
                         uint32_t opaque,
                         Vbid vbucket,
                         const DocKeyView& key,
                         uint64_t prepared_seqno,
                         uint64_t abort_seqno);

std::pair<cb::engine_errc, cb::rangescan::Id> createRangeScan(
        Cookie& cookie, const cb::rangescan::CreateParameters& params);

cb::engine_errc continueRangeScan(
        Cookie& cookie, const cb::rangescan::ContinueParameters& params);

cb::engine_errc cancelRangeScan(Cookie& cookie,
                                Vbid vbid,
                                cb::rangescan::Id uuid);

cb::engine_errc bucket_set_parameter(Cookie& cookie,
                                     EngineParamCategory category,
                                     std::string_view key,
                                     std::string_view value,
                                     Vbid vbucket);

cb::engine_errc bucket_compact_database(Cookie& cookie);

std::pair<cb::engine_errc, vbucket_state_t> bucket_get_vbucket(Cookie& cookie);

cb::engine_errc bucket_set_vbucket(Cookie& cookie,
                                   vbucket_state_t state,
                                   nlohmann::json& meta);
cb::engine_errc bucket_delete_vbucket(Cookie& cookie, Vbid vbid, bool sync);
