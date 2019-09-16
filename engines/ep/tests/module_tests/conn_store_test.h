/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2019 Couchbase, Inc
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

#include "../mock/mock_synchronous_ep_engine.h"
#include "evp_store_single_threaded_test.h"

#include <folly/portability/GTest.h>

class MockConnStore;

/**
 * Unit tests for the ConnStore class. Requires an ep engine instance to make
 * ConnHandler instances, using SingleThreadedKVBucketTest for that.
 */
class ConnStoreTest : public SingleThreadedKVBucketTest {
public:
    void SetUp() override;
    void TearDown() override;

protected:
    std::shared_ptr<ConnHandler> addConnHandler(const void* cookie,
                                                const std::string& name);

    void removeConnHandler(const void* cookie);

    void addVbConn(Vbid vb, ConnHandler& conn);
    void removeVbConn(Vbid vb, const void* cookie);

    std::unique_ptr<MockConnStore> connStore;
};
