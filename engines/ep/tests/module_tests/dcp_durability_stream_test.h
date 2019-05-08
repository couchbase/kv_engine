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

#include "dcp_stream_test.h"

/*
 * ActiveStream tests for Durability. Single-threaded.
 */
class DurabilityActiveStreamTest : public SingleThreadedActiveStreamTest {
public:
    void SetUp() override;
    void TearDown() override;

protected:
    /*
     * Queues a Prepare and verifies that the corresponding DCP_PREPARE
     * message has been queued into the ActiveStream::readyQ.
     */
    void testSendDcpPrepare();
};

/*
 * PassiveStream tests for Durability. Single-threaded.
 */
class DurabilityPassiveStreamTest : public SingleThreadedPassiveStreamTest {
public:
    void SetUp() override;
    void TearDown() override;

protected:
    /*
     * Simulates a Replica receiving a DCP_PREPARE and checks that it is
     * queued correctly for persistence.
     */
    void testReceiveDcpPrepare();
};

/**
 * PassiveStream tests for Durability against persistent buckets.
 * Single-threaded.
 */
class DurabilityPassiveStreamPersistentTest
    : public DurabilityPassiveStreamTest {};
