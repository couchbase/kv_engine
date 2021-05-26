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

#include "mock_stream.h"
#include "checkpoint_manager.h"
#include "dcp/response.h"
#include "mock_dcp_producer.h"
#include "vbucket.h"
MockActiveStream::MockActiveStream(
        EventuallyPersistentEngine* e,
        std::shared_ptr<MockDcpProducer> p,
        uint32_t flags,
        uint32_t opaque,
        VBucket& vb,
        uint64_t st_seqno,
        uint64_t en_seqno,
        uint64_t vb_uuid,
        uint64_t snap_start_seqno,
        uint64_t snap_end_seqno,
        IncludeValue includeValue,
        IncludeXattrs includeXattrs,
        IncludeDeletedUserXattrs includeDeletedUserXattrs)
    : ActiveStream(e,
                   p,
                   p->getName(),
                   flags,
                   opaque,
                   vb,
                   st_seqno,
                   en_seqno,
                   vb_uuid,
                   snap_start_seqno,
                   snap_end_seqno,
                   includeValue,
                   includeXattrs,
                   IncludeDeleteTime::No,
                   includeDeletedUserXattrs,
                   {{}, vb.getManifest()}) {
}

void MockActiveStream::public_registerCursor(CheckpointManager& manager,
                                             const std::string& name,
                                             int64_t seqno) {
    auto registerResult = manager.registerCursorBySeqno(name, seqno);
    cursor = registerResult.cursor;
}

std::unique_ptr<DcpResponse> MockActiveStream::public_popFromReadyQ() {
    std::lock_guard<std::mutex> lg(streamMutex);
    return popFromReadyQ();
}

std::unique_ptr<DcpResponse> MockActiveStream::public_nextQueuedItem() {
    LockHolder lh(streamMutex);
    return nextQueuedItem();
}

std::unique_ptr<DcpResponse> MockActiveStream::public_makeResponseFromItem(
        queued_item& item, SendCommitSyncWriteAs sendMutationInsteadOfCommit) {
    return makeResponseFromItem(item, sendMutationInsteadOfCommit);
}

void MockActiveStream::consumeBackfillItems(int numItems) {
    std::lock_guard<std::mutex> lh(streamMutex);
    for (int items = 0; items < numItems;) {
        auto resp = backfillPhase(lh);
        if (resp) {
            ++items;
        }
    }
}
void MockActiveStream::consumeAllBackfillItems() {
    std::lock_guard<std::mutex> lh(streamMutex);
    while (backfillPhase(lh)) {
    }
}

ENGINE_ERROR_CODE MockPassiveStream::messageReceived(
        std::unique_ptr<DcpResponse> dcpResponse) {
    return PassiveStream::messageReceived(std::move(dcpResponse));
}

std::unique_ptr<DcpResponse> MockPassiveStream::public_popFromReadyQ() {
    return popFromReadyQ();
}
