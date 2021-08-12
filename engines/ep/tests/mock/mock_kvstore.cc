/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2021 Couchbase, Inc
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

#include "mock_kvstore.h"
#include "kv_bucket.h"
#include "vb_commit.h"
#include <mcbp/protocol/request.h>

using namespace ::testing;

MockKVStore::MockKVStore() {
    // By default, allow a scanContext to be constructed and destroyed.
    ON_CALL(*this, initBySeqnoScanContext(_, _, _, _, _, _, _))
            .WillByDefault(Invoke([this](auto cb,
                                         auto cl,
                                         Vbid vbid,
                                         uint64_t startSeqno,
                                         DocumentFilter options,
                                         ValueFilter valOptions,
                                         SnapshotSource source) {
                vbucket_state state;
                state.maxVisibleSeqno = 1;
                std::vector<Collections::KVStore::DroppedCollection> dropped;
                return std::make_unique<BySeqnoScanContext>(
                        std::move(cb),
                        std::move(cl),
                        vbid,
                        std::make_unique<KVFileHandle>(),
                        startSeqno,
                        1,
                        0,
                        options,
                        valOptions,
                        1,
                        state,
                        dropped,
                        std::optional<uint64_t>{});
            }));
}

MockKVStore::~MockKVStore() = default;

MockKVStore& MockKVStore::replaceROKVStoreWithMock(KVBucket& bucket,
                                                   size_t shardId) {
    auto rwro = bucket.takeRWRO(0);
    auto ro = std::make_unique<MockKVStore>();
    auto& mockKVStore = dynamic_cast<MockKVStore&>(*ro);
    bucket.setRWRO(0, std::move(rwro.rw), std::move(ro));
    return mockKVStore;
}
