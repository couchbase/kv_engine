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

/**
 * This file contains wrapper functions on top of the engine interface.
 * If you want to know more information of a function or the arguments
 * it takes, you should look in `memcached/engine.h`.
 *
 * The `handle` and `cookie` parameter in the engine interface methods is
 * replaced by the connection object.
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

ENGINE_ERROR_CODE bucket_unknown_command(Cookie& cookie,
                                         const AddResponseFn& response);

void bucket_item_set_cas(Connection& c, gsl::not_null<item*> it, uint64_t cas);

void bucket_item_set_datatype(Connection& c,
                              gsl::not_null<item*> it,
                              protocol_binary_datatype_t datatype);

void bucket_reset_stats(Cookie& cookie);

bool bucket_get_item_info(Connection& c,
                          gsl::not_null<const item*> item_,
                          gsl::not_null<item_info*> item_info_);

cb::EngineErrorMetadataPair bucket_get_meta(Cookie& cookie,
                                            const DocKey& key,
                                            Vbid vbucket);

ENGINE_ERROR_CODE bucket_store(
        Cookie& cookie,
        gsl::not_null<item*> item_,
        uint64_t& cas,
        ENGINE_STORE_OPERATION operation,
        boost::optional<cb::durability::Requirements> durability,
        DocumentState document_state = DocumentState::Alive);

cb::EngineErrorCasPair bucket_store_if(
        Cookie& cookie,
        gsl::not_null<item*> item_,
        uint64_t cas,
        ENGINE_STORE_OPERATION operation,
        cb::StoreIfPredicate predicate,
        boost::optional<cb::durability::Requirements> durability,
        DocumentState document_state = DocumentState::Alive);

ENGINE_ERROR_CODE bucket_remove(
        Cookie& cookie,
        const DocKey& key,
        uint64_t& cas,
        Vbid vbucket,
        boost::optional<cb::durability::Requirements> durability,
        mutation_descr_t& mut_info);

cb::EngineErrorItemPair bucket_get(
        Cookie& cookie,
        const DocKey& key,
        Vbid vbucket,
        DocStateFilter documentStateFilter = DocStateFilter::Alive);

cb::EngineErrorItemPair bucket_get_if(
        Cookie& cookie,
        const DocKey& key,
        Vbid vbucket,
        std::function<bool(const item_info&)> filter);

cb::EngineErrorItemPair bucket_get_and_touch(
        Cookie& cookie,
        const DocKey& key,
        Vbid vbucket,
        uint32_t expiration,
        boost::optional<cb::durability::Requirements> durability);

BucketCompressionMode bucket_get_compression_mode(Cookie& cookie);

size_t bucket_get_max_item_size(Cookie& cookie);

float bucket_min_compression_ratio(Cookie& cookie);

cb::EngineErrorItemPair bucket_get_locked(Cookie& cookie,
                                          const DocKey& key,
                                          Vbid vbucket,
                                          uint32_t lock_timeout);

ENGINE_ERROR_CODE bucket_unlock(Cookie& cookie,
                                const DocKey& key,
                                Vbid vbucket,
                                uint64_t cas);

std::pair<cb::unique_item_ptr, item_info> bucket_allocate_ex(Cookie& cookie,
                                                             const DocKey& key,
                                                             size_t nbytes,
                                                             size_t priv_nbytes,
                                                             int flags,
                                                             rel_time_t exptime,
                                                             uint8_t datatype,
                                                             Vbid vbucket);

ENGINE_ERROR_CODE bucket_flush(Cookie& cookie);

ENGINE_ERROR_CODE bucket_get_stats(Cookie& cookie,
                                   cb::const_char_buffer key,
                                   cb::const_byte_buffer value,
                                   const AddStatFn& add_stat);

/**
 * Calls the underlying engine DCP add-stream
 *
 * @param cookie The cookie representing the connection
 * @param opaque The opaque field in the received message
 * @param vbid The vbucket id
 * @param flags
 * @return ENGINE_ERROR_CODE
 */
ENGINE_ERROR_CODE dcpAddStream(Cookie& cookie,
                               uint32_t opaque,
                               Vbid vbid,
                               uint32_t flags);

/**
 * Calls the underlying engine DCP buffer-acknowledgement
 *
 * @param cookie The cookie representing the connection
 * @param opaque The opaque field in the received message
 * @param vbid The vbucket id
 * @param ackSize The number of acknowledged bytes
 * @return ENGINE_ERROR_CODE
 */
ENGINE_ERROR_CODE dcpBufferAcknowledgement(Cookie& cookie,
                                           uint32_t opaque,
                                           Vbid vbid,
                                           uint32_t ackSize);

/**
 * Calls the underlying engine DCP close-stream
 *
 * @param cookie The cookie representing the connection
 * @param opaque The opaque field in the received message
 * @param vbid The vbucket id
 * @param sid The ID of the stream
 * @return ENGINE_ERROR_CODE
 */
ENGINE_ERROR_CODE dcpCloseStream(Cookie& cookie,
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
 * @return ENGINE_ERROR_CODE
 */
ENGINE_ERROR_CODE dcpControl(Cookie& cookie,
                             uint32_t opaque,
                             cb::const_char_buffer key,
                             cb::const_char_buffer val);

/**
 * Calls the underlying engine DCP deletion
 *
 * @param cookie The cookie representing the connection
 * @param opaque The opaque field in the received message
 * @param key The document key
 * @param value The document value to store
 * @param privilegedPoolSize The number of bytes in the value which should be
 *                   allocated from the privileged pool
 * @param datatype The document datatype
 * @param cas The documents CAS
 * @param vbid The vbucket id
 * @param bySeqno The db sequence number
 * @param revSeqno The revision sequence number
 * @param meta The document meta
 * @return ENGINE_ERROR_CODE
 */
ENGINE_ERROR_CODE dcpDeletion(Cookie& cookie,
                              uint32_t opaque,
                              const DocKey& key,
                              cb::const_byte_buffer value,
                              size_t privilegedPoolSize,
                              uint8_t datatype,
                              uint64_t cas,
                              Vbid vbid,
                              uint64_t bySeqno,
                              uint64_t revSeqno,
                              cb::const_byte_buffer meta);

/**
 * Calls the underlying engine DCP deletion v2
 *
 * @param cookie The cookie representing the connection
 * @param opaque The opaque field in the received message
 * @param key The document key
 * @param value The document value to store
 * @param privilegedPoolSize The number of bytes in the value which should be
 *                   allocated from the privileged pool
 * @param datatype The document datatype
 * @param cas The documents CAS
 * @param vbid The vbucket id
 * @param bySeqno The db sequence number
 * @param revSeqno The revision sequence number
 * @param deleteTime The time of the deletion
 * @return ENGINE_ERROR_CODE
 */
ENGINE_ERROR_CODE dcpDeletionV2(Cookie& cookie,
                                uint32_t opaque,
                                const DocKey& key,
                                cb::const_byte_buffer value,
                                size_t priv_bytes,
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
 * @param privilegedPoolSize The number of bytes in the value which should be
 *                   allocated from the privileged pool
 * @param datatype The document datatype
 * @param cas The documents CAS
 * @param vbid The vbucket id
 * @param bySeqno The db sequence number
 * @param revSeqno The revision sequence number
 * @param deleteTime The time of the deletion
 * @return ENGINE_ERROR_CODE
 */
ENGINE_ERROR_CODE dcpExpiration(Cookie& cookie,
                                uint32_t opaque,
                                const DocKey& key,
                                cb::const_byte_buffer value,
                                size_t privilegedPoolSize,
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
 * @return ENGINE_ERROR_CODE
 */
ENGINE_ERROR_CODE dcpGetFailoverLog(Cookie& cookie,
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
 * @param privilegedPoolSize The number of bytes in the value which should be
 *                   allocated from the privileged pool
 * @param datatype The document datatype
 * @param cas The documents CAS
 * @param vbid The vbucket id
 * @param flags
 * @param bySeqno The db sequence number
 * @param revSeqno The revision sequence number
 * @param expiration The document expiration
 * @param lockTime The document lock time
 * @param meta The document meta
 * @param nru The document NRU
 * @return ENGINE_ERROR_CODE
 */
ENGINE_ERROR_CODE dcpMutation(Cookie& cookie,
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
                              uint8_t nru);

/**
 * Calls the underlying engine DCP noop
 *
 * @param cookie The cookie representing the connection
 * @param opaque The opaque field in the received message
 * @return ENGINE_ERROR_CODE
 */
ENGINE_ERROR_CODE dcpNoop(Cookie& cookie, uint32_t opaque);

/**
 * Calls the underlying engine DCP open
 *
 * @param cookie The cookie representing the connection
 * @param opaque The opaque field in the received message
 * @param seqno
 * @param flags The DCP open flags
 * @param name The DCP connection name
 * @return ENGINE_ERROR_CODE
 */
ENGINE_ERROR_CODE dcpOpen(Cookie& cookie,
                          uint32_t opaque,
                          uint32_t seqno,
                          uint32_t flags,
                          cb::const_char_buffer name,
                          cb::const_char_buffer value);

/**
 * Calls the underlying engine DCP set-vbucket-state
 *
 * @param cookie The cookie representing the connection
 * @param opaque The opaque field in the received message
 * @param vbid The vbucket id
 * @param state The new vbucket state
 * @return ENGINE_ERROR_CODE
 */
ENGINE_ERROR_CODE dcpSetVbucketState(Cookie& cookie,
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
 * @param maxVisibleSeqno The snapshot's maximum visible seqno
 *
 * @return ENGINE_ERROR_CODE
 */
ENGINE_ERROR_CODE dcpSnapshotMarker(
        Cookie& cookie,
        uint32_t opaque,
        Vbid vbid,
        uint64_t startSeqno,
        uint64_t endSeqno,
        uint32_t flags,
        boost::optional<uint64_t> highCompletedSeqno,
        boost::optional<uint64_t> maxVisibleSeqno);

/**
 * Calls the underlying engine DCP stream-end
 *
 * @param cookie The cookie representing the connection
 * @param opaque The opaque field in the received message
 * @param vbid The vbucket id
 * @param flags
 * @return ENGINE_ERROR_CODE
 */
ENGINE_ERROR_CODE dcpStreamEnd(Cookie& cookie,
                               uint32_t opaque,
                               Vbid vbid,
                               uint32_t flags);

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
 * @return ENGINE_ERROR_CODE
 */
ENGINE_ERROR_CODE dcpStreamReq(Cookie& cookie,
                               uint32_t flags,
                               uint32_t opaque,
                               Vbid vbid,
                               uint64_t startSeqno,
                               uint64_t endSeqno,
                               uint64_t vbucketUuid,
                               uint64_t snapStartSeqno,
                               uint64_t snapEndSeqno,
                               uint64_t* rollbackSeqno,
                               dcp_add_failover_log callback,
                               boost::optional<cb::const_char_buffer> json);

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
 * @return ENGINE_ERROR_CODE
 */
ENGINE_ERROR_CODE dcpSystemEvent(Cookie& cookie,
                                 uint32_t opaque,
                                 Vbid vbucket,
                                 mcbp::systemevent::id eventId,
                                 uint64_t bySeqno,
                                 mcbp::systemevent::version version,
                                 cb::const_byte_buffer eventKey,
                                 cb::const_byte_buffer eventData);

ENGINE_ERROR_CODE dcpPrepare(Cookie& cookie,
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
                             cb::durability::Level level);

ENGINE_ERROR_CODE dcpSeqnoAcknowledged(Cookie& cookie,
                                       uint32_t opaque,
                                       Vbid vbucket,
                                       uint64_t prepared_seqno);

ENGINE_ERROR_CODE dcpCommit(Cookie& cookie,
                            uint32_t opaque,
                            Vbid vbucket,
                            const DocKey& key,
                            uint64_t prepared_seqno,
                            uint64_t commit_seqno);

ENGINE_ERROR_CODE dcpAbort(Cookie& cookie,
                           uint32_t opaque,
                           Vbid vbucket,
                           const DocKey& key,
                           uint64_t prepared_seqno,
                           uint64_t abort_seqno);
