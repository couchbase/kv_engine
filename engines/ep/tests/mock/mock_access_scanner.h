/*
 *     Copyright 2023-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */

#pragma once

#include "access_scanner.h"

/***
 * Test class to expose the behaviour needed to create an ItemAccessVisitor
 */
class MockAccessScanner : public AccessScanner {
public:
    MockAccessScanner(KVBucket& _store,
                      Configuration& conf,
                      EPStats& st,
                      double sleeptime = 0,
                      bool useStartTime = false,
                      bool completeBeforeShutdown = false)
        : AccessScanner(_store,
                        conf,
                        st,
                        sleeptime,
                        useStartTime,
                        completeBeforeShutdown) {
    }

    void public_createAndScheduleTask(const size_t shard) {
        return createAndScheduleTask(shard);
    }
};
