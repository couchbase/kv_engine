/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2019-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */

#include "mock_dcp_consumer.h"
#include "dcp/response.h"
#include "mock_stream.h"

MockDcpConsumer::MockDcpConsumer(EventuallyPersistentEngine& theEngine,
                                 const void* cookie,
                                 const std::string& name,
                                 const std::string& consumerName)
    : DcpConsumer(theEngine, cookie, name, consumerName) {
}

std::shared_ptr<PassiveStream> MockDcpConsumer::makePassiveStream(
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
        const Collections::ManifestUid vb_manifest_uid) {
    return std::make_shared<MockPassiveStream>(e,
                                               consumer,
                                               name,
                                               flags,
                                               opaque,
                                               vb,
                                               start_seqno,
                                               end_seqno,
                                               vb_uuid,
                                               snap_start_seqno,
                                               snap_end_seqno,
                                               vb_high_seqno,
                                               vb_manifest_uid);
}

std::optional<uint32_t> MockDcpConsumer::getStreamOpaque(uint32_t opaque) {
    for (const auto& pair : opaqueMap_) {
        if (pair.second.first == opaque) {
            return pair.first;
        }
    }
    return {};
}
