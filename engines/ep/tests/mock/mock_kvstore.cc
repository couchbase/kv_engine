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

MockKVStore::MockKVStore(std::unique_ptr<KVStoreIface> real)
    : realKVS(std::move(real)) {
    if (realKVS) {
        // If we have a real KVStore, delegate some common methods to it
        // to aid in mocking.
        // Note: this could probably be expanded to the entire interface,
        // however thus far only methods needed by unit tests using the mock
        // have been implemented.
        ON_CALL(*this, initBySeqnoScanContext(_, _, _, _, _, _, _))
                .WillByDefault([this](auto cb,
                                      auto cl,
                                      Vbid vbid,
                                      uint64_t startSeqno,
                                      DocumentFilter options,
                                      ValueFilter valOptions,
                                      SnapshotSource source) {
                    return this->realKVS->initBySeqnoScanContext(std::move(cb),
                                                                 std::move(cl),
                                                                 vbid,
                                                                 startSeqno,
                                                                 options,
                                                                 valOptions,
                                                                 source);
                });
        ON_CALL(*this, getCachedVBucketState(_))
                .WillByDefault([this](Vbid vbid) {
                    return this->realKVS->getCachedVBucketState(vbid);
                });
        ON_CALL(*this, getAggrDbFileInfo()).WillByDefault([this]() {
            return this->realKVS->getAggrDbFileInfo();
        });
        ON_CALL(*this, getConfig()).WillByDefault([this]() {
            return this->realKVS->getConfig();
        });
    }
}

MockKVStore::~MockKVStore() = default;

MockKVStore& MockKVStore::replaceRWKVStoreWithMock(KVBucket& bucket,
                                                   size_t shardId) {
    auto rw = bucket.takeRW(0);
    auto mockRw = std::make_unique<MockKVStore>(std::move(rw));
    auto& mockKVStore = dynamic_cast<MockKVStore&>(*mockRw);
    bucket.setRW(0, std::move(mockRw));
    return mockKVStore;
}

std::unique_ptr<MockKVStore> MockKVStore::restoreOriginalRWKVStore(
        KVBucket& bucket) {
    auto rw = bucket.takeRW(0);
    // Sanity check - read-write from bucket should be an instance of
    // MockKVStore
    if (!dynamic_cast<MockKVStore*>(rw.get())) {
        throw std::logic_error(
                "MockKVStore::restoreOriginalRWKVStore: Bucket's read-write "
                "KVS is not an instance of MockKVStore");
    }
    // Take ownership of the MockKVStore from the bucket.
    auto ownedMockKVS = std::unique_ptr<MockKVStore>(
            dynamic_cast<MockKVStore*>(rw.release()));
    // Put real KVStore back into bucket, return mock.
    bucket.setRW(0, std::move(ownedMockKVS->realKVS));
    return ownedMockKVS;
}
