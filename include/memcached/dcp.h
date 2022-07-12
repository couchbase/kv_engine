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

#include <gsl/gsl-lite.hpp>
#include <mcbp/protocol/dcp_stream_end_status.h>
#include <mcbp/protocol/status.h>
#include <memcached/dcp_stream_id.h>
#include <memcached/engine_error.h>
#include <memcached/types.h>
#include <memcached/vbucket.h>
#include <memcached/visibility.h>

class CookieIface;
struct DocKey;

namespace cb::durability {
class Requirements;
enum class Level : uint8_t;
} // namespace cb::durability

namespace cb::mcbp {
class Response;
}

namespace mcbp::systemevent {
enum class id : uint32_t;
enum class version : uint8_t;
} // namespace mcbp::systemevent

class DcpConnHandlerIface {
public:
    virtual ~DcpConnHandlerIface() = default;
};

/**
 * The message producers are used by the engine's DCP producer
 * to add messages into the DCP stream.  Please look at the full
 * DCP documentation to figure out the real meaning for all of the
 * messages.
 */
struct DcpMessageProducersIface {
    virtual ~DcpMessageProducersIface() = default;

    virtual cb::engine_errc get_failover_log(uint32_t opaque, Vbid vbucket) = 0;

    virtual cb::engine_errc stream_req(uint32_t opaque,
                                       Vbid vbucket,
                                       uint32_t flags,
                                       uint64_t start_seqno,
                                       uint64_t end_seqno,
                                       uint64_t vbucket_uuid,
                                       uint64_t snap_start_seqno,
                                       uint64_t snap_end_seqno,
                                       const std::string& request_value) = 0;

    virtual cb::engine_errc add_stream_rsp(uint32_t opaque,
                                           uint32_t stream_opaque,
                                           cb::mcbp::Status status) = 0;

    virtual cb::engine_errc marker_rsp(uint32_t opaque,
                                       cb::mcbp::Status status) = 0;

    virtual cb::engine_errc set_vbucket_state_rsp(uint32_t opaque,
                                                  cb::mcbp::Status status) = 0;

    /**
     * Send a Stream End message
     *
     * @param opaque this is the opaque requested by the consumer
     *               in the Stream Request message
     * @param vbucket the vbucket id the message belong to
     * @param status the reason for the stream end.
     *              0 = success
     *              1+ = Something happened on the vbucket causing
     *                   us to abort it.
     * @param sid The stream-ID the end applies to (can be 0 for none)
     *
     * @return cb::engine_errc::success upon success
     *         cb::engine_errc::would_block if no data is available
     *         ENGINE_* for errors
     */
    virtual cb::engine_errc stream_end(uint32_t opaque,
                                       Vbid vbucket,
                                       cb::mcbp::DcpStreamEndStatus status,
                                       cb::mcbp::DcpStreamId sid) = 0;

    /**
     * Send a marker
     *
     * @param opaque this is the opaque requested by the consumer
     *               in the Stream Request message
     * @param vbucket the vbucket id the message belong to
     * @param start_seqno start of the snapshot range
     * @param end_seqno end of the snapshot range
     * @param flags snapshot marker flags (DISK/MEMORY/CHK/ACK).
     * @param highCompletedSeqno the SyncRepl high completed seqno
     * @param maxVisibleSeqno highest committed seqno (ignores prepare/abort)
     * @param timestamp for the data in the snapshot marker (only valid for
     *                  disk type and represents the disk commit time)
     * @param sid The stream-ID the marker applies to (can be 0 for none)
     *
     * @return cb::engine_errc::success upon success
     */
    virtual cb::engine_errc marker(uint32_t opaque,
                                   Vbid vbucket,
                                   uint64_t start_seqno,
                                   uint64_t end_seqno,
                                   uint32_t flags,
                                   std::optional<uint64_t> highCompletedSeqno,
                                   std::optional<uint64_t> maxVisibleSeqno,
                                   std::optional<uint64_t> timestamp,
                                   cb::mcbp::DcpStreamId sid) = 0;

    /**
     * Send a Mutation
     *
     * @param opaque this is the opaque requested by the consumer
     *               in the Stream Request message
     * @param itm the item to send.
     * @param vbucket the vbucket id the message belong to
     * @param by_seqno
     * @param rev_seqno
     * @param lock_time
     * @param nru the nru field used by ep-engine (may safely be ignored)
     * @param sid The stream-ID the mutation applies to (can be 0 for none)
     *
     * @return cb::engine_errc::success upon success
     */
    virtual cb::engine_errc mutation(uint32_t opaque,
                                     cb::unique_item_ptr itm,
                                     Vbid vbucket,
                                     uint64_t by_seqno,
                                     uint64_t rev_seqno,
                                     uint32_t lock_time,
                                     uint8_t nru,
                                     cb::mcbp::DcpStreamId sid) = 0;

    /**
     * Send a deletion
     *
     * @param opaque this is the opaque requested by the consumer
     *               in the Stream Request message
     * @param itm the item to send.
     * @param by_seqno
     * @param rev_seqno
     * @param sid The stream-ID the deletion applies to (can be 0 for none)
     *
     * @return cb::engine_errc::success upon success
     */
    virtual cb::engine_errc deletion(uint32_t opaque,
                                     cb::unique_item_ptr itm,
                                     Vbid vbucket,
                                     uint64_t by_seqno,
                                     uint64_t rev_seqno,
                                     cb::mcbp::DcpStreamId sid) = 0;

    /**
     * Send a deletion with delete_time or collections (or both)
     *
     * @param opaque this is the opaque requested by the consumer
     *               in the Stream Request message
     * @param itm the item to send.
     * @param vbucket the vbucket id the message belong to
     * @param by_seqno
     * @param rev_seqno
     * @param delete_time the time of the deletion (tombstone creation time)
     * @param sid The stream-ID the deletion applies to (can be 0 for none)
     *
     * @return cb::engine_errc::success upon success
     */
    virtual cb::engine_errc deletion_v2(uint32_t opaque,
                                        cb::unique_item_ptr itm,
                                        Vbid vbucket,
                                        uint64_t by_seqno,
                                        uint64_t rev_seqno,
                                        uint32_t delete_time,
                                        cb::mcbp::DcpStreamId sid) = 0;

    /**
     * Send an expiration
     *
     * @param opaque this is the opaque requested by the consumer
     *               in the Stream Request message
     * @param itm the item to send.
     * @param by_seqno
     * @param rev_seqno
     * @param delete_time the time of the deletion (tombstone creation time)
     * @param sid The stream-ID the expiration applies to (can be 0 for none)
     *
     * @return cb::engine_errc::success upon success
     */
    virtual cb::engine_errc expiration(uint32_t opaque,
                                       cb::unique_item_ptr itm,
                                       Vbid vbucket,
                                       uint64_t by_seqno,
                                       uint64_t rev_seqno,
                                       uint32_t delete_time,
                                       cb::mcbp::DcpStreamId sid) = 0;

    /**
     * Send a state transition for a vbucket
     *
     * @param opaque this is the opaque requested by the consumer
     *               in the Stream Request message
     * @param vbucket the vbucket id the message belong to
     * @param state the new state
     *
     * @return cb::engine_errc::success upon success
     */
    virtual cb::engine_errc set_vbucket_state(uint32_t opaque,
                                              Vbid vbucket,
                                              vbucket_state_t state) = 0;

    /**
     * Send a noop
     *
     * @param opaque what to use as the opaque in the buffer
     *
     * @return cb::engine_errc::success upon success
     */
    virtual cb::engine_errc noop(uint32_t opaque) = 0;

    /**
     * Send a buffer acknowledgment
     *
     * @param opaque this is the opaque requested by the consumer
     *               in the Stream Request message
     * @param buffer_bytes the amount of bytes processed
     *
     * @return cb::engine_errc::success upon success
     */
    virtual cb::engine_errc buffer_acknowledgement(uint32_t opaque,
                                                   uint32_t buffer_bytes) = 0;

    /**
     * Send a control message to the other end
     *
     * @param opaque what to use as the opaque in the buffer
     * @param key the identifier for the property to set
     * @param value The value for the property (the layout of the
     *              value is defined for the key)
     *
     * @return cb::engine_errc::success upon success
     */
    virtual cb::engine_errc control(uint32_t opaque,
                                    std::string_view key,
                                    std::string_view value) = 0;

    /**
     * Send a system event message to the other end
     *
     * @param cookie passed on the cookie provided by step
     * @param opaque what to use as the opaque in the buffer
     * @param vbucket the vbucket the event applies to
     * @param bySeqno the sequence number of the event
     * @param version A version value defining the eventData format
     * @param key the system event's key data
     * @param eventData the system event's specific data
     * @param sid The stream-ID the event applies to (can be 0 for none)
     *
     * @return cb::engine_errc::success upon success
     */
    virtual cb::engine_errc system_event(uint32_t opaque,
                                         Vbid vbucket,
                                         mcbp::systemevent::id event,
                                         uint64_t bySeqno,
                                         mcbp::systemevent::version version,
                                         cb::const_byte_buffer key,
                                         cb::const_byte_buffer eventData,
                                         cb::mcbp::DcpStreamId sid) = 0;

    /*
     * Send a GetErrorMap message to the other end
     *
     * @param opaque The opaque to send over
     * @param version The version of the error map
     *
     * @return cb::engine_errc::success upon success
     */
    virtual cb::engine_errc get_error_map(uint32_t opaque,
                                          uint16_t version) = 0;

    /**
     * See mutation for a description of the parameters except for:
     *
     * @param deleted Are we storing a deletion operation?
     * @param durability the durability specification for this item
     */
    virtual cb::engine_errc prepare(uint32_t opaque,
                                    cb::unique_item_ptr itm,
                                    Vbid vbucket,
                                    uint64_t by_seqno,
                                    uint64_t rev_seqno,
                                    uint32_t lock_time,
                                    uint8_t nru,
                                    DocumentState document_state,
                                    cb::durability::Level level) = 0;

    /**
     * Send a seqno ack message
     *
     * It serves to inform KV-Engine active nodes that the replica has
     * successfully received and prepared to memory/disk all DCP_PREPARE
     * messages up to the specified seqno.
     *
     * @param opaque identifying stream
     * @param vbucket the vbucket the seqno ack is for
     * @param prepared_seqno The seqno the replica has prepared up to.
     */
    virtual cb::engine_errc seqno_acknowledged(uint32_t opaque,
                                               Vbid vbucket,
                                               uint64_t prepared_seqno) = 0;

    /**
     * Send a commit message:
     *
     * This is sent from the DCP Producer to the DCP Consumer.
     * It is only sent to DCP replicas, not GSI, FTS etc. It serves to inform
     * KV-Engine replicas of a committed Sync Write
     *
     * @param opaque
     * @param vbucket the vbucket the event applies to
     * @param key The key of the committed mutation.
     * @param prepare_seqno The seqno of the prepare that we are committing.
     * @param commit_seqno The sequence number to commit this mutation at.
     * @return
     */
    virtual cb::engine_errc commit(uint32_t opaque,
                                   Vbid vbucket,
                                   const DocKey& key,
                                   uint64_t prepare_seqno,
                                   uint64_t commit_seqno) = 0;
    /**
     * Send an abort message:
     *
     * This is sent from the DCP Producer to the DCP Consumer.
     */
    virtual cb::engine_errc abort(uint32_t opaque,
                                  Vbid vbucket,
                                  const DocKey& key,
                                  uint64_t prepared_seqno,
                                  uint64_t abort_seqno) = 0;
    /**
     * Send an OSO snapshot marker to from server to client
     */
    virtual cb::engine_errc oso_snapshot(uint32_t opaque,
                                         Vbid vbucket,
                                         uint32_t flags,
                                         cb::mcbp::DcpStreamId sid) = 0;

    virtual cb::engine_errc seqno_advanced(uint32_t opaque,
                                           Vbid vbucket,
                                           uint64_t seqno,
                                           cb::mcbp::DcpStreamId sid) = 0;
};

using dcp_add_failover_log =
        std::function<cb::engine_errc(const std::vector<vbucket_failover_t>&)>;

struct MEMCACHED_PUBLIC_CLASS DcpIface {
    /**
     * Called from the memcached core for a DCP connection to allow it to
     * inject new messages on the stream.
     *
     * @param cookie a unique handle the engine should pass on to the
     *               message producers
     * @param throttled set to true if the connection is currently throttle
     *                  and in this case you're not allowed to send new
     *                  mutations / expiry / deletions (only control
     *                  messages)
     * @param producers functions the client may use to add messages to
     *                  the DCP stream
     *
     * @return The appropriate error code returned from the message
     *         producerif it failed, or:
     *         cb::engine_errc::success if the engine don't have more messages
     *                        to send at this moment
     */
    virtual cb::engine_errc step(const CookieIface& cookie,
                                 bool throttled,
                                 DcpMessageProducersIface& producers) = 0;

    /**
     * Called from the memcached core to open a new DCP connection.
     *
     * @param cookie a unique handle the engine should pass on to the
     *               message producers (typically representing the memcached
     *               connection).
     * @param opaque what to use as the opaque for this DCP connection.
     * @param seqno Unused
     * @param flags bitfield of flags to specify what to open. See DCP_OPEN_XXX
     * @param name Identifier for this connection. Note that the name must be
     *             unique; attempting to (re)connect with a name already in use
     *             will disconnect the existing connection.
     * @param value An optional JSON value specifying extra information about
     *              the connection to be opened.
     * @return cb::engine_errc::success if the DCP connection was successfully
     * opened, otherwise error code indicating reason for the failure.
     */
    virtual cb::engine_errc open(const CookieIface& cookie,
                                 uint32_t opaque,
                                 uint32_t seqno,
                                 uint32_t flags,
                                 std::string_view name,
                                 std::string_view value = {}) = 0;

    /**
     * Called from the memcached core to add a vBucket stream to the set of
     * connected streams.
     *
     * @param cookie a unique handle the engine should pass on to the
     *               message producers (typically representing the memcached
     *               connection).
     * @param opaque what to use as the opaque for this DCP connection.
     * @param vbucket The vBucket to stream.
     * @param flags bitfield of flags to specify what to open. See
     *              DCP_ADD_STREAM_FLAG_XXX
     * @return cb::engine_errc::success if the DCP stream was successfully
     * opened, otherwise error code indicating reason for the failure.
     */
    virtual cb::engine_errc add_stream(const CookieIface& cookie,
                                       uint32_t opaque,
                                       Vbid vbucket,
                                       uint32_t flags) = 0;

    /**
     * Called from the memcached core to close a vBucket stream to the set of
     * connected streams.
     *
     * @param cookie a unique handle the engine should pass on to the
     *               message producers (typically representing the memcached
     *               connection).
     * @param opaque what to use as the opaque for this DCP connection.
     * @param vbucket The vBucket to close.
     * @param sid The id of the stream to close (can be 0/none)
     * @return
     */
    virtual cb::engine_errc close_stream(const CookieIface& cookie,
                                         uint32_t opaque,
                                         Vbid vbucket,
                                         cb::mcbp::DcpStreamId sid) = 0;

    /**
     * Callback to the engine that a Stream Request message was received
     */
    virtual cb::engine_errc stream_req(
            const CookieIface& cookie,
            uint32_t flags,
            uint32_t opaque,
            Vbid vbucket,
            uint64_t start_seqno,
            uint64_t end_seqno,
            uint64_t vbucket_uuid,
            uint64_t snap_start_seqno,
            uint64_t snap_end_seqno,
            uint64_t* rollback_seqno,
            dcp_add_failover_log callback,
            std::optional<std::string_view> json) = 0;

    /**
     * Callback to the engine that a get failover log message was received
     */
    virtual cb::engine_errc get_failover_log(const CookieIface& cookie,
                                             uint32_t opaque,
                                             Vbid vbucket,
                                             dcp_add_failover_log callback) = 0;

    /**
     * Callback to the engine that a stream end message was received
     */
    virtual cb::engine_errc stream_end(const CookieIface& cookie,
                                       uint32_t opaque,
                                       Vbid vbucket,
                                       cb::mcbp::DcpStreamEndStatus status) = 0;

    /**
     * Callback to the engine that a snapshot marker message was received
     */
    virtual cb::engine_errc snapshot_marker(
            const CookieIface& cookie,
            uint32_t opaque,
            Vbid vbucket,
            uint64_t start_seqno,
            uint64_t end_seqno,
            uint32_t flags,
            std::optional<uint64_t> high_completed_seqno,
            std::optional<uint64_t> max_visible_seqno) = 0;

    /**
     * Callback to the engine that a mutation message was received
     *
     * @param cookie The cookie representing the connection
     * @param opaque The opaque field in the message (identifying the stream)
     * @param key The documents key
     * @param value The value to store
     * @param priv_bytes The number of bytes in the value which should be
     *                   allocated from the privileged pool
     * @param datatype The datatype for the incomming item
     * @param cas The documents CAS value
     * @param vbucket The vbucket identifier for the document
     * @param flags The user specified flags
     * @param by_seqno The sequence number in the vbucket
     * @param rev_seqno The revision number for the item
     * @param expiration When the document expire
     * @param lock_time The lock time for the document
     * @param meta The documents meta
     * @param nru The engine's NRU value
     * @return Standard engine error code.
     */
    virtual cb::engine_errc mutation(const CookieIface& cookie,
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
                                     cb::const_byte_buffer meta,
                                     uint8_t nru) = 0;

    /**
     * Callback to the engine that a deletion message was received
     *
     * @param cookie The cookie representing the connection
     * @param opaque The opaque field in the message (identifying the stream)
     * @param key The documents key
     * @param value The value to store
     * @param priv_bytes The number of bytes in the value which should be
     *                   allocated from the privileged pool
     * @param datatype The datatype for the incomming item
     * @param cas The documents CAS value
     * @param vbucket The vbucket identifier for the document
     * @param by_seqno The sequence number in the vbucket
     * @param rev_seqno The revision number for the item
     * @param meta The documents meta
     * @return Standard engine error code.
     */
    virtual cb::engine_errc deletion(const CookieIface& cookie,
                                     uint32_t opaque,
                                     const DocKey& key,
                                     cb::const_byte_buffer value,
                                     size_t priv_bytes,
                                     uint8_t datatype,
                                     uint64_t cas,
                                     Vbid vbucket,
                                     uint64_t by_seqno,
                                     uint64_t rev_seqno,
                                     cb::const_byte_buffer meta) = 0;

    /**
     * Callback to the engine that a deletion_v2 message was received
     *
     * @param cookie The cookie representing the connection
     * @param opaque The opaque field in the message (identifying the stream)
     * @param key The documents key
     * @param value The value to store
     * @param priv_bytes The number of bytes in the value which should be
     *                   allocated from the privileged pool
     * @param datatype The datatype for the incomming item
     * @param cas The documents CAS value
     * @param vbucket The vbucket identifier for the document
     * @param by_seqno The sequence number in the vbucket
     * @param rev_seqno The revision number for the item
     * @param delete_time The time of the delete
     * @return Standard engine error code.
     */
    virtual cb::engine_errc deletion_v2(const CookieIface& cookie,
                                        uint32_t opaque,
                                        const DocKey& key,
                                        cb::const_byte_buffer value,
                                        size_t priv_bytes,
                                        uint8_t datatype,
                                        uint64_t cas,
                                        Vbid vbucket,
                                        uint64_t by_seqno,
                                        uint64_t rev_seqno,
                                        uint32_t delete_time) {
        return cb::engine_errc::not_supported;
    }

    /**
     * Callback to the engine that an expiration message was received
     *
     * @param cookie The cookie representing the connection
     * @param opaque The opaque field in the message (identifying the stream)
     * @param key The documents key
     * @param value The value to store
     * @param priv_bytes The number of bytes in the value which should be
     *                   allocated from the privileged pool
     * @param datatype The datatype for the incomming item
     * @param cas The documents CAS value
     * @param vbucket The vbucket identifier for the document
     * @param by_seqno The sequence number in the vbucket
     * @param rev_seqno The revision number for the item
     * @param meta The documents meta
     * @return Standard engine error code.
     */
    virtual cb::engine_errc expiration(const CookieIface& cookie,
                                       uint32_t opaque,
                                       const DocKey& key,
                                       cb::const_byte_buffer value,
                                       size_t priv_bytes,
                                       uint8_t datatype,
                                       uint64_t cas,
                                       Vbid vbucket,
                                       uint64_t by_seqno,
                                       uint64_t rev_seqno,
                                       uint32_t deleteTime) = 0;

    /**
     * Callback to the engine that a set vbucket state message was received
     */
    virtual cb::engine_errc set_vbucket_state(const CookieIface& cookie,
                                              uint32_t opaque,
                                              Vbid vbucket,
                                              vbucket_state_t state) = 0;

    /**
     * Callback to the engine that a NOOP message was received
     */
    virtual cb::engine_errc noop(const CookieIface& cookie,
                                 uint32_t opaque) = 0;

    /**
     * Callback to the engine that a buffer_ack message was received
     */
    virtual cb::engine_errc buffer_acknowledgement(const CookieIface& cookie,
                                                   uint32_t opaque,
                                                   uint32_t buffer_bytes) = 0;

    /**
     * Callback to the engine that a Control message was received.
     *
     * @param cookie The cookie representing the connection
     * @param opaque The opaque field in the message (identifying the stream)
     * @param key The control message name
     * @param value The control message value
     * @return Standard engine error code.
     */
    virtual cb::engine_errc control(const CookieIface& cookie,
                                    uint32_t opaque,
                                    std::string_view key,
                                    std::string_view value) = 0;

    /**
     * Callback to the engine that a response message has been received.
     * @param cookie The cookie representing the connection
     * @param response The response which the server received.
     * @return Standard engine error code.
     */
    virtual cb::engine_errc response_handler(
            const CookieIface& cookie, const cb::mcbp::Response& response) = 0;

    /**
     * Callback to the engine that a system event message was received.
     *
     * @param cookie The cookie representing the connection
     * @param opaque The opaque field in the message (identifying the stream)
     * @param vbucket The vbucket identifier for this event.
     * @param event The type of system event.
     * @param bySeqno Sequence number of event.
     * @param version The version of system event (defines the eventData format)
     * @param key The event name .
     * @param eventData The event value.
     * @return Standard engine error code.
     */
    virtual cb::engine_errc system_event(const CookieIface& cookie,
                                         uint32_t opaque,
                                         Vbid vbucket,
                                         mcbp::systemevent::id event,
                                         uint64_t bySeqno,
                                         mcbp::systemevent::version version,
                                         cb::const_byte_buffer key,
                                         cb::const_byte_buffer eventData) = 0;

    /**
     * Called by the core when it receives a DCP PREPARE message over the
     * wire.
     *
     * See mutation for a description of the parameters except for:
     *
     * @param deleted Are we storing a deletion operation?
     * @param durability the durability specification for this item
     */
    virtual cb::engine_errc prepare(const CookieIface& cookie,
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
                                    cb::durability::Level level) = 0;

    /**
     * Called by the core when it receives a DCP SEQNO ACK message over the
     * wire.
     *
     * It serves to inform KV-Engine active nodes that the replica has
     * successfully received and prepared all DCP_PREPARE messages up to the
     * specified seqno.
     *
     * @param cookie connection to send it over
     * @param opaque identifying stream
     * @param vbucket The vbucket which is being acknowledged.
     * @param prepared_seqno The seqno the replica has prepared up to.
     */
    virtual cb::engine_errc seqno_acknowledged(const CookieIface& cookie,
                                               uint32_t opaque,
                                               Vbid vbucket,
                                               uint64_t prepared_seqno) = 0;

    /**
     * Called by the core when it receives a DCP COMMIT message over the
     * wire.
     *
     * This is sent from the DCP Producer to the DCP Consumer.
     * It is only sent to DCP replicas, not GSI, FTS etc. It serves to inform
     * KV-Engine replicas of a committed Sync Write
     */
    virtual cb::engine_errc commit(const CookieIface& cookie,
                                   uint32_t opaque,
                                   Vbid vbucket,
                                   const DocKey& key,
                                   uint64_t prepared_seqno,
                                   uint64_t commit_seqno) = 0;
    /**
     * Called by the core when it receives a DCP ABORT message over the
     * wire.
     *
     * This is sent from the DCP Producer to the DCP Consumer.
     */
    virtual cb::engine_errc abort(const CookieIface& cookie,
                                  uint32_t opaque,
                                  Vbid vbucket,
                                  const DocKey& key,
                                  uint64_t prepared_seqno,
                                  uint64_t abort_seqno) = 0;
};
