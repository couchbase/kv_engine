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

#include <mcbp/protocol/status.h>
#include <memcached/dcp_stream_id.h>
#include <memcached/engine_error.h>
#include <memcached/types.h>
#include <memcached/vbucket.h>
#include <memcached/visibility.h>
#include <gsl/gsl>

struct DocKey;
union protocol_binary_request_header;
union protocol_binary_response_header;

namespace cb {
namespace durability {
class Requirements;
enum class Level : uint8_t;
}
} // namespace cb

namespace mcbp {
namespace systemevent {
enum class id : uint32_t;
enum class version : uint8_t;
}

} // namespace mcbp

/**
 * The message producers are used by the engine's DCP producer
 * to add messages into the DCP stream.  Please look at the full
 * DCP documentation to figure out the real meaning for all of the
 * messages.
 */
struct dcp_message_producers {
    virtual ~dcp_message_producers() = default;

    virtual ENGINE_ERROR_CODE get_failover_log(uint32_t opaque,
                                               Vbid vbucket) = 0;

    virtual ENGINE_ERROR_CODE stream_req(uint32_t opaque,
                                         Vbid vbucket,
                                         uint32_t flags,
                                         uint64_t start_seqno,
                                         uint64_t end_seqno,
                                         uint64_t vbucket_uuid,
                                         uint64_t snap_start_seqno,
                                         uint64_t snap_end_seqno,
                                         const std::string& request_value) = 0;

    virtual ENGINE_ERROR_CODE add_stream_rsp(uint32_t opaque,
                                             uint32_t stream_opaque,
                                             cb::mcbp::Status status) = 0;

    virtual ENGINE_ERROR_CODE marker_rsp(uint32_t opaque,
                                         cb::mcbp::Status status) = 0;

    virtual ENGINE_ERROR_CODE set_vbucket_state_rsp(
            uint32_t opaque, cb::mcbp::Status status) = 0;

    /**
     * Send a Stream End message
     *
     * @param opaque this is the opaque requested by the consumer
     *               in the Stream Request message
     * @param vbucket the vbucket id the message belong to
     * @param flags the reason for the stream end.
     *              0 = success
     *              1 = Something happened on the vbucket causing
     *                  us to abort it.
     * @param sid The stream-ID the end applies to (can be 0 for none)
     *
     * @return ENGINE_SUCCESS upon success
     *         ENGINE_EWOULDBLOCK if no data is available
     *         ENGINE_* for errors
     */
    virtual ENGINE_ERROR_CODE stream_end(uint32_t opaque,
                                         Vbid vbucket,
                                         uint32_t flags,
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
     * @param sid The stream-ID the marker applies to (can be 0 for none)
     *
     * @return ENGINE_SUCCESS upon success
     */
    virtual ENGINE_ERROR_CODE marker(
            uint32_t opaque,
            Vbid vbucket,
            uint64_t start_seqno,
            uint64_t end_seqno,
            uint32_t flags,
            boost::optional<uint64_t> highCompletedSeqno,
            boost::optional<uint64_t> maxVisibleSeqno,
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
     * @return ENGINE_SUCCESS upon success
     */
    virtual ENGINE_ERROR_CODE mutation(uint32_t opaque,
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
     * @return ENGINE_SUCCESS upon success
     */
    virtual ENGINE_ERROR_CODE deletion(uint32_t opaque,
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
     * @return ENGINE_SUCCESS upon success
     */
    virtual ENGINE_ERROR_CODE deletion_v2(uint32_t opaque,
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
     * @return ENGINE_SUCCESS upon success
     */
    virtual ENGINE_ERROR_CODE expiration(uint32_t opaque,
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
     * @return ENGINE_SUCCESS upon success
     */
    virtual ENGINE_ERROR_CODE set_vbucket_state(uint32_t opaque,
                                                Vbid vbucket,
                                                vbucket_state_t state) = 0;

    /**
     * Send a noop
     *
     * @param opaque what to use as the opaque in the buffer
     *
     * @return ENGINE_SUCCESS upon success
     */
    virtual ENGINE_ERROR_CODE noop(uint32_t opaque) = 0;

    /**
     * Send a buffer acknowledgment
     *
     * @param opaque this is the opaque requested by the consumer
     *               in the Stream Request message
     * @param vbucket the vbucket id the message belong to
     * @param buffer_bytes the amount of bytes processed
     *
     * @return ENGINE_SUCCESS upon success
     */
    virtual ENGINE_ERROR_CODE buffer_acknowledgement(uint32_t opaque,
                                                     Vbid vbucket,
                                                     uint32_t buffer_bytes) = 0;

    /**
     * Send a control message to the other end
     *
     * @param opaque what to use as the opaque in the buffer
     * @param key the identifier for the property to set
     * @param value The value for the property (the layout of the
     *              value is defined for the key)
     *
     * @return ENGINE_SUCCESS upon success
     */
    virtual ENGINE_ERROR_CODE control(uint32_t opaque,
                                      cb::const_char_buffer key,
                                      cb::const_char_buffer value) = 0;

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
     * @return ENGINE_SUCCESS upon success
     */
    virtual ENGINE_ERROR_CODE system_event(uint32_t opaque,
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
     * @return ENGINE_SUCCESS upon success
     */
    virtual ENGINE_ERROR_CODE get_error_map(uint32_t opaque,
                                            uint16_t version) = 0;

    /**
     * See mutation for a description of the parameters except for:
     *
     * @param deleted Are we storing a deletion operation?
     * @param durability the durability specification for this item
     */
    virtual ENGINE_ERROR_CODE prepare(uint32_t opaque,
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
    virtual ENGINE_ERROR_CODE seqno_acknowledged(uint32_t opaque,
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
    virtual ENGINE_ERROR_CODE commit(uint32_t opaque,
                                     Vbid vbucket,
                                     const DocKey& key,
                                     uint64_t prepare_seqno,
                                     uint64_t commit_seqno) = 0;
    /**
     * Send an abort message:
     *
     * This is sent from the DCP Producer to the DCP Consumer.
     */
    virtual ENGINE_ERROR_CODE abort(uint32_t opaque,
                                    Vbid vbucket,
                                    const DocKey& key,
                                    uint64_t prepared_seqno,
                                    uint64_t abort_seqno) = 0;
    /**
     * Send an OSO snapshot marker to from server to client
     */
    virtual ENGINE_ERROR_CODE oso_snapshot(uint32_t opaque,
                                           Vbid vbucket,
                                           uint32_t flags,
                                           cb::mcbp::DcpStreamId sid) = 0;
};

typedef ENGINE_ERROR_CODE (*dcp_add_failover_log)(
        vbucket_failover_t*,
        size_t nentries,
        gsl::not_null<const void*> cookie);

struct MEMCACHED_PUBLIC_CLASS DcpIface {
    /**
     * Called from the memcached core for a DCP connection to allow it to
     * inject new messages on the stream.
     *
     * @param cookie a unique handle the engine should pass on to the
     *               message producers
     * @param producers functions the client may use to add messages to
     *                  the DCP stream
     *
     * @return The appropriate error code returned from the message
     *         producerif it failed, or:
     *         ENGINE_SUCCESS if the engine don't have more messages
     *                        to send at this moment
     */
    virtual ENGINE_ERROR_CODE step(
            gsl::not_null<const void*> cookie,
            gsl::not_null<dcp_message_producers*> producers) = 0;

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
     * @return ENGINE_SUCCESS if the DCP connection was successfully opened,
     *         otherwise error code indicating reason for the failure.
     */
    virtual ENGINE_ERROR_CODE open(gsl::not_null<const void*> cookie,
                                   uint32_t opaque,
                                   uint32_t seqno,
                                   uint32_t flags,
                                   cb::const_char_buffer name,
                                   cb::const_char_buffer value = {}) = 0;

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
     * @return ENGINE_SUCCESS if the DCP stream was successfully opened,
     *         otherwise error code indicating reason for the failure.
     */
    virtual ENGINE_ERROR_CODE add_stream(gsl::not_null<const void*> cookie,
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
    virtual ENGINE_ERROR_CODE close_stream(gsl::not_null<const void*> cookie,
                                           uint32_t opaque,
                                           Vbid vbucket,
                                           cb::mcbp::DcpStreamId sid) = 0;

    /**
     * Callback to the engine that a Stream Request message was received
     */
    virtual ENGINE_ERROR_CODE stream_req(
            gsl::not_null<const void*> cookie,
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
            boost::optional<cb::const_char_buffer> json) = 0;

    /**
     * Callback to the engine that a get failover log message was received
     */
    virtual ENGINE_ERROR_CODE get_failover_log(
            gsl::not_null<const void*> cookie,
            uint32_t opaque,
            Vbid vbucket,
            dcp_add_failover_log callback) = 0;

    /**
     * Callback to the engine that a stream end message was received
     */
    virtual ENGINE_ERROR_CODE stream_end(gsl::not_null<const void*> cookie,
                                         uint32_t opaque,
                                         Vbid vbucket,
                                         uint32_t flags) = 0;

    /**
     * Callback to the engine that a snapshot marker message was received
     */
    virtual ENGINE_ERROR_CODE snapshot_marker(
            gsl::not_null<const void*> cookie,
            uint32_t opaque,
            Vbid vbucket,
            uint64_t start_seqno,
            uint64_t end_seqno,
            uint32_t flags,
            boost::optional<uint64_t> high_completed_seqno,
            boost::optional<uint64_t> max_visible_seqno) = 0;

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
    virtual ENGINE_ERROR_CODE mutation(gsl::not_null<const void*> cookie,
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
    virtual ENGINE_ERROR_CODE deletion(gsl::not_null<const void*> cookie,
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
    virtual ENGINE_ERROR_CODE deletion_v2(gsl::not_null<const void*> cookie,
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
        return ENGINE_ENOTSUP;
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
    virtual ENGINE_ERROR_CODE expiration(gsl::not_null<const void*> cookie,
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
    virtual ENGINE_ERROR_CODE set_vbucket_state(
            gsl::not_null<const void*> cookie,
            uint32_t opaque,
            Vbid vbucket,
            vbucket_state_t state) = 0;

    /**
     * Callback to the engine that a NOOP message was received
     */
    virtual ENGINE_ERROR_CODE noop(gsl::not_null<const void*> cookie,
                                   uint32_t opaque) = 0;

    /**
     * Callback to the engine that a buffer_ack message was received
     */
    virtual ENGINE_ERROR_CODE buffer_acknowledgement(
            gsl::not_null<const void*> cookie,
            uint32_t opaque,
            Vbid vbucket,
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
    virtual ENGINE_ERROR_CODE control(gsl::not_null<const void*> cookie,
                                      uint32_t opaque,
                                      cb::const_char_buffer key,
                                      cb::const_char_buffer value) = 0;

    /**
     * Callback to the engine that a response message has been received.
     * @param cookie The cookie representing the connection
     * @param response The response which the server received.
     * @return Standard engine error code.
     */
    virtual ENGINE_ERROR_CODE response_handler(
            gsl::not_null<const void*> cookie,
            const protocol_binary_response_header* response) = 0;

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
    virtual ENGINE_ERROR_CODE system_event(gsl::not_null<const void*> cookie,
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
    virtual ENGINE_ERROR_CODE prepare(gsl::not_null<const void*> cookie,
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
    virtual ENGINE_ERROR_CODE seqno_acknowledged(
            gsl::not_null<const void*> cookie,
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
    virtual ENGINE_ERROR_CODE commit(gsl::not_null<const void*> cookie,
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
    virtual ENGINE_ERROR_CODE abort(gsl::not_null<const void*> cookie,
                                    uint32_t opaque,
                                    Vbid vbucket,
                                    const DocKey& key,
                                    uint64_t prepared_seqno,
                                    uint64_t abort_seqno) = 0;
};
