/*
 *     Copyright 2022-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */

#include "item_eviction_strategy.h"

ItemEvictionStrategy::~ItemEvictionStrategy() = default;

std::unique_ptr<ItemEvictionStrategy> ItemEvictionStrategy::evict_nothing() {
    class EvictNothing : public ItemEvictionStrategy {
    public:
        void setupVBucketVisit(uint64_t /*numExpectedItems*/) override {
            // do nothing
        }

        void tearDownVBucketVisit(vbucket_state_t /*state*/) override {
            // do nothing
        }

        bool shouldTryEvict(uint8_t freq,
                            uint64_t age,
                            vbucket_state_t state) override {
            // don't try evict anything
            return false;
        }

        void eligibleItemSeen(uint8_t /*freq*/,
                              uint64_t /*age*/,
                              vbucket_state_t /*state*/) override {
            // do nothing
        }
    };

    return std::make_unique<EvictNothing>();
}

std::unique_ptr<ItemEvictionStrategy> ItemEvictionStrategy::evict_everything() {
    class EvictNothing : public ItemEvictionStrategy {
    public:
        void setupVBucketVisit(uint64_t /*numExpectedItems*/) override {
            // do nothing
        }

        void tearDownVBucketVisit(vbucket_state_t /*state*/) override {
            // do nothing
        }

        bool shouldTryEvict(uint8_t freq,
                            uint64_t age,
                            vbucket_state_t state) override {
            // try to evict every item
            return true;
        }

        void eligibleItemSeen(uint8_t /*freq*/,
                              uint64_t /*age*/,
                              vbucket_state_t /*state*/) override {
            // do nothing
        }
    };

    return std::make_unique<EvictNothing>();
}
