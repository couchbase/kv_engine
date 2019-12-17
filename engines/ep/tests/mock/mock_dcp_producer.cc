/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2018 Couchbase, Inc.
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

#include "mock_dcp_producer.h"

#include "mock_stream.h"

std::shared_ptr<MockActiveStream> MockDcpProducer::mockActiveStreamRequest(
        uint32_t flags,
        uint32_t opaque,
        VBucket& vb,
        uint64_t start_seqno,
        uint64_t end_seqno,
        uint64_t vbucket_uuid,
        uint64_t snap_start_seqno,
        uint64_t snap_end_seqno,
        IncludeValue includeValue,
        IncludeXattrs includeXattrs) {
    auto stream = std::make_shared<MockActiveStream>(
            static_cast<EventuallyPersistentEngine*>(&engine_),
            std::static_pointer_cast<MockDcpProducer>(shared_from_this()),
            flags,
            opaque,
            vb,
            start_seqno,
            end_seqno,
            vbucket_uuid,
            snap_start_seqno,
            snap_end_seqno,
            includeValue,
            includeXattrs);
    stream->setActive();
    if (!streams.insert(std::make_pair(vb.getId(), stream))) {
        throw std::logic_error(
                "MockDcpProducer::mockActiveStreamRequest "
                "failed to insert requested stream");
    }
    notifyStreamReady(vb.getId());
    return stream;
}