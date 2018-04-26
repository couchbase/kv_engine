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

void MockDcpProducer::mockActiveStreamRequest(uint32_t flags,
                                              uint32_t opaque,
                                              uint16_t vbucket,
                                              uint64_t start_seqno,
                                              uint64_t end_seqno,
                                              uint64_t vbucket_uuid,
                                              uint64_t snap_start_seqno,
                                              uint64_t snap_end_seqno) {
    stream_t stream = new MockActiveStream(
            static_cast<EventuallyPersistentEngine*>(&engine_),
            this,
            getName(),
            flags,
            opaque,
            vbucket,
            start_seqno,
            end_seqno,
            vbucket_uuid,
            snap_start_seqno,
            snap_end_seqno);
    stream->setActive();
    auto insertResult = streams.insert(std::make_pair(vbucket, stream));
    if (!insertResult.second) {
        throw std::logic_error("MockDcpProducer::mockActiveStreamRequest "
                               "failed to insert requested stream");
    }
    notifyStreamReady(vbucket);
}
