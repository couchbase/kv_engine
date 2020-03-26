/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2019 Couchbase, Inc
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
