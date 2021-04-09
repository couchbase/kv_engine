/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2020-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */
#pragma once

#include <engines/ep/tests/module_tests/collections/collections_dcp_test.h>
#include <engines/ep/tests/module_tests/evp_store_durability_test.h>

struct failover_entry_t;

class CollectionsSyncWriteParamTest : public CollectionsDcpParameterizedTest {
public:
    void SetUp() override;
    void TearDown() override;

protected:
    failover_entry_t
    testCompleteDifferentPrepareOnActiveBeforeReplicaDropSetUp();
};
