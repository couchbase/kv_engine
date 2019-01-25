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
#include "vbucket.h"

MockActiveStream::MockActiveStream(EventuallyPersistentEngine* e,
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
                                   IncludeXattrs includeXattrs)
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
                   {{}, vb.getManifest()}) {
}

void MockActiveStream::public_registerCursor(CheckpointManager& manager,
                                             const std::string& name,
                                             int64_t seqno) {
    auto registerResult = manager.registerCursorBySeqno(name, seqno);
    cursor = registerResult.cursor;
}
