/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2016-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */

#pragma once

#include "dcp/consumer.h"

#include <optional>

/*
 * Mock of the DcpConsumer class.  Wraps the real DcpConsumer class
 * and provides get/set access to lastMessageTime.
 */
class MockDcpConsumer: public DcpConsumer {
public:
    MockDcpConsumer(EventuallyPersistentEngine& theEngine,
                    const void* cookie,
                    const std::string& name,
                    const std::string& consumerName = {});

    void setPendingAddStream(bool value) {
        // The unit tests was written before memcached marked the
        // connection as DCP as part of a successfull DCP Open (and wouldn't
        // call step() before add stream was received.
        //
        // Some of the tests we've got just want to verify other things
        // so let's allow them to fake that the message was received
        pendingAddStream = value;
    }

    void setLastMessageTime(const rel_time_t timeValue) {
        lastMessageTime = timeValue;
    }

    rel_time_t getLastMessageTime() {
        return lastMessageTime;
    }

    std::shared_ptr<PassiveStream> getVbucketStream(Vbid vbid) {
        return findStream(vbid);
    }

    void public_notifyVbucketReady(Vbid vbid) {
        notifyVbucketReady(vbid);
    }

    void setNumBackoffs(uint32_t v) {
        backoffs = v;
    }

    uint32_t getNumBackoffs() const {
        return backoffs.load();
    }

    GetErrorMapState getGetErrorMapState() {
        return getErrorMapState;
    }

    bool getProducerIsVersion5orHigher() {
        return producerIsVersion5orHigher;
    }

    FlowControl& getFlowControl() {
        return flowControl;
    }

    /*
     * Creates a PassiveStream.
     * @return a SingleThreadedRCPtr to the newly created MockPassiveStream.
     */
    std::shared_ptr<PassiveStream> makePassiveStream(
            EventuallyPersistentEngine& e,
            std::shared_ptr<DcpConsumer> consumer,
            const std::string& name,
            uint32_t flags,
            uint32_t opaque,
            Vbid vb,
            uint64_t start_seqno,
            uint64_t end_seqno,
            uint64_t vb_uuid,
            uint64_t snap_start_seqno,
            uint64_t snap_end_seqno,
            uint64_t vb_high_seqno,
            const Collections::ManifestUid vb_manifest_uid) override;

    /**
     * @return the opaque sent to the Producer as part of the DCP_CONTROL
     *     request for the Sync Replication negotiation
     */
    uint32_t public_getSyncReplNegotiationOpaque() const {
        return syncReplNegotiation.opaque;
    }

    /**
     * @return the entire SyncReplNegotiation struct used to send DCP_CONTROL
     *         messages for testing.
     */
    BlockingDcpControlNegotiation public_getSyncReplNegotiation() const {
        return syncReplNegotiation;
    }

    bool public_getPendingSendConsumerName() const {
        return pendingSendConsumerName;
    }

    /**
     * Used for simulating a successful Consumer-Producer Sync Repl handshake.
     */
    void enableSyncReplication() {
        supportsSyncReplication = SyncReplication::SyncReplication;
    }

    /**
     * Disable SyncRepl for testing
     */
    void disableSyncReplication() {
        supportsSyncReplication = SyncReplication::No;
    }

    /**
     * @return the entire v7 DCP status code Negotiation struct used to send
     * DCP_CONTROL messages for testing.
     */
    BlockingDcpControlNegotiation public_getV7StatusCodesNegotiation() const {
        return v7DcpStatusCodesNegotiation;
    }

    /**
     * Enable the use of V7 status codes for DCP in tests
     */
    void enableV7DcpStatus() {
        isV7DcpStatusEnabled = true;
    }

    /**
     *
     * Map from the opaque used to create a stream to the internal opaque
     * as is used by streamEnd
     *
     * DcpConsumer maintains an internal opaque map; rather than assuming
     * the value that will be used for the opaque of a given stream, use this
     * map to get the correct value.
     */
    std::optional<uint32_t> getStreamOpaque(uint32_t opaque);

    void public_streamAccepted(uint32_t opaque,
                               cb::mcbp::Status status,
                               const uint8_t* body,
                               uint32_t bodylen) {
        streamAccepted(opaque, status, body, bodylen);
    }

    BlockingDcpControlNegotiation public_getDeletedUserXattrsNegotiation()
            const {
        return deletedUserXattrsNegotiation;
    }

    IncludeDeletedUserXattrs public_getIncludeDeletedUserXattrs() const {
        return includeDeletedUserXattrs;
    }

    void public_setIncludeDeletedUserXattrs(IncludeDeletedUserXattrs value) {
        includeDeletedUserXattrs = value;
    }

    FlowControl& public_flowControl() {
        return flowControl;
    }
};
