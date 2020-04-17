/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2018 Couchbase, Inc
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

#include "dcp/stream.h"
#include "spdlog/common.h"

class BucketLogger;

class NotifierStream : public Stream {
public:
    /// The states this NotifierStream object can be in.
    enum class StreamState { Pending, Dead };

    NotifierStream(EventuallyPersistentEngine* e,
                   std::shared_ptr<DcpProducer> producer,
                   const std::string& name,
                   uint32_t flags,
                   uint32_t opaque,
                   Vbid vb,
                   uint64_t start_seqno,
                   uint64_t end_seqno,
                   uint64_t vb_uuid,
                   uint64_t snap_start_seqno,
                   uint64_t snap_end_seqno);

    std::unique_ptr<DcpResponse> next() override;

    uint32_t setDead(end_stream_status_t status) override;

    /// @returns true if state_ is not Dead
    bool isActive() const override;

    void notifySeqnoAvailable(uint64_t seqno) override;

    std::string getStreamTypeName() const override;

    std::string getStateName() const override;

    void addStats(const AddStatFn& add_stat, const void* c) override;

    static std::string to_string(StreamState type);

    void closeIfRequiredPrivilegesLost(const void* cookie) override {
        // @todo: MB-38829 require bucket DcpStream privilege
    }

private:
    void transitionState(StreamState newState);

    template <typename... Args>
    void log(spdlog::level::level_enum severity,
             const char* fmt,
             Args... args) const;

    /**
     * Notifies the producer connection that the stream has items ready to be
     * pick up.
     */
    void notifyStreamReady();

    std::atomic<StreamState> state_{StreamState::Pending};

    const std::weak_ptr<DcpProducer> producerPtr;
};
