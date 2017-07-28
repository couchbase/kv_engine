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

#include "dcp/consumer.h"
#include "mock_stream.h"

/*
 * Mock of the DcpConsumer class.  Wraps the real DcpConsumer class
 * and provides get/set access to lastMessageTime.
 */
class MockDcpConsumer: public DcpConsumer {
public:
    MockDcpConsumer(EventuallyPersistentEngine &theEngine, const void *cookie,
                    const std::string &name)
    : DcpConsumer(theEngine, cookie, name)
    {}

    void setLastMessageTime(const rel_time_t timeValue) {
        lastMessageTime = timeValue;
    }

    rel_time_t getLastMessageTime() {
        return lastMessageTime;
    }

    SingleThreadedRCPtr<PassiveStream> getVbucketStream(uint16_t vbid) {
        return findStream(vbid);
    }

    void public_notifyVbucketReady(uint16_t vbid) {
        notifyVbucketReady(vbid);
    }

    uint32_t getNumBackoffs() const {
        return backoffs.load();
    }

    /*
     * Creates a PassiveStream.
     * @return a SingleThreadedRCPtr to the newly created MockPassiveStream.
     */
    SingleThreadedRCPtr<PassiveStream> makePassiveStream(
            EventuallyPersistentEngine& e,
            dcp_consumer_t consumer,
            const std::string& name,
            uint32_t flags,
            uint32_t opaque,
            uint16_t vb,
            uint64_t start_seqno,
            uint64_t end_seqno,
            uint64_t vb_uuid,
            uint64_t snap_start_seqno,
            uint64_t snap_end_seqno,
            uint64_t vb_high_seqno) override {
        return SingleThreadedRCPtr<PassiveStream>(
                new MockPassiveStream(e,
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
                                      vb_high_seqno));
    }

};
