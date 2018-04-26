/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2016 Couchbase, Inc.
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

#include "dcp/producer.h"
#include "dcp/stream.h"

/*
 * Mock of the DcpProducer class.  Wraps the real DcpProducer, but exposes
 * normally protected methods publically for test purposes.
 */
class MockDcpProducer: public DcpProducer {
public:
    MockDcpProducer(EventuallyPersistentEngine &theEngine, const void *cookie,
                    const std::string &name, bool isNotifier)
    : DcpProducer(theEngine, cookie, name, isNotifier)
    {}

    ENGINE_ERROR_CODE maybeSendNoop(struct dcp_message_producers* producers)
    {
        return DcpProducer::maybeSendNoop(producers);
    }

    void setNoopSendTime(const rel_time_t timeValue) {
        noopCtx.sendTime = timeValue;
    }

    rel_time_t getNoopSendTime() {
        return noopCtx.sendTime;
    }

    bool getNoopPendingRecv() {
        return noopCtx.pendingRecv;
    }

    void setNoopEnabled(const bool booleanValue) {
        noopCtx.enabled = booleanValue;
    }

    bool getNoopEnabled() {
        return noopCtx.enabled;
    }

    ActiveStreamCheckpointProcessorTask& getCheckpointSnapshotTask() const {
        return *static_cast<ActiveStreamCheckpointProcessorTask*>(
                        checkpointCreatorTask.get());
    }

    /**
     * Finds the stream for a given vbucket
     */
    SingleThreadedRCPtr<Stream> findStream(uint16_t vbid) {
        auto it = streams.find(vbid);
        if (it != streams.end()) {
            return it->second;
        } else {
            return SingleThreadedRCPtr<Stream>();
        }
    }

    /**
     * Place a mock active stream into the producer
     */
    void mockActiveStreamRequest(uint32_t flags,
                                 uint32_t opaque,
                                 uint16_t vbucket,
                                 uint64_t start_seqno,
                                 uint64_t end_seqno,
                                 uint64_t vbucket_uuid,
                                 uint64_t snap_start_seqno,
                                 uint64_t snap_end_seqno);
};

using mock_dcp_producer_t = SingleThreadedRCPtr<MockDcpProducer>;
