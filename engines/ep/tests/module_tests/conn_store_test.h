/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2019-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
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
