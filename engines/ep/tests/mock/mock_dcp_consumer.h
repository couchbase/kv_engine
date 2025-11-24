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
                    CookieIface* cookie,
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

    void setLastMessageTime(cb::time::steady_clock::time_point timeValue) {
        lastMessageTime = timeValue;
    }

    cb::time::steady_clock::time_point getLastMessageTime() {
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

    FlowControl& getFlowControl() {
        return flowControl;
    }

    void setSyncReplication(SyncReplication value) {
        supportsSyncReplication = value;
    }

    bool public_isSyncReplication() const {
        return supportsSyncReplication.load() ==
               SyncReplication::SyncReplication;
    }

    /*
     * Creates a PassiveStream.
     * @return a SingleThreadedRCPtr to the newly created MockPassiveStream.
     */
    std::shared_ptr<PassiveStream> makePassiveStream(
            EventuallyPersistentEngine& e,
            std::shared_ptr<DcpConsumer> consumer,
            const std::string& name,
            cb::mcbp::DcpAddStreamFlag flags,
            uint32_t opaque,
            Vbid vb,
            uint64_t start_seqno,
            uint64_t vb_uuid,
            uint64_t snap_start_seqno,
            uint64_t snap_end_seqno,
            uint64_t vb_high_seqno,
            const Collections::ManifestUid vb_manifest_uid) override;

    const BlockingDcpControlNegotiation& public_getCurrentControlNegotiation()
            const {
        if (pendingControls.lock()->empty()) {
            throw std::logic_error(
                    "MockDcpConsumer::public_getCurrentControlNegotiation: "
                    "pendingControls is empty");
        }
        return pendingControls.lock()->front();
    }

    const auto& public_getPendingControls() const {
        return pendingControls;
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
     * Enable the use of V7 status codes for DCP in tests
     */
    void enableV7DcpStatus() {
        useDcpV7StatusCodes = true;
    }

    bool useV7StatusCodes() const {
        return useDcpV7StatusCodes;
    }

    bool isOpaqueBlockingDcpControl(uint64_t opaque) const {
        return pendingControls.withLock([opaque](const auto& container) {
            if (container.empty()) {
                return false;
            }

            return container.front().opaque == opaque;
        });
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
                               cb::const_byte_buffer newFailoverLog) {
        streamAccepted(opaque, status, newFailoverLog);
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

    void disableFlatBuffersSystemEvents() {
        flatBuffersSystemEventsEnabled = false;
    }

    // Set FlatBuffers configuration without the full control tx/rx loop
    void enableFlatBuffersSystemEvents() {
        flatBuffersSystemEventsEnabled = true;
    }

    /**
     * Proxy to Consumer::processMutationOrPrepare()
     */
    cb::engine_errc public_processMutationOrPrepare(Vbid vbid,
                                                    uint32_t opaque,
                                                    const DocKeyView& key,
                                                    queued_item item,
                                                    size_t msgBytes);

    /**
     * Proxy to Consumer::findStream()
     */
    std::shared_ptr<PassiveStream> public_findStream(Vbid vbid);
};
