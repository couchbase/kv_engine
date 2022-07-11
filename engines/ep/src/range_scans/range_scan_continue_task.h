/*
 *     Copyright 2022-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */

#pragma once

#include <executor/globaltask.h>
#include <memcached/vbucket.h>

class EPBucket;
class RangeScan;

/**
 * RangeScanContinueTask runs the second step of a range scan
 * - continuing the scan (reading data)
 * - cancelling the scan (closing the file)
 *
 * The continuing step can occur many times depending on scan size and any
 * limits. A completed scan will also self-cancel on this task
 */
class RangeScanContinueTask : public GlobalTask {
public:
    RangeScanContinueTask(EPBucket& bucket);

    /**
     * run will locate a RangeScan and depending on the state continue or
     * cancel
     */
    bool run() override;

    std::string getDescription() const override {
        return "RangeScanContinueTask";
    }

    std::chrono::microseconds maxExpectedDuration() const override {
        // @todo: tune based on real data
        return std::chrono::seconds(1);
    }

protected:
    /**
     * Continue the range scan and if it completes/fails cancel the scan so no
     * further continues can occur.
     */
    void continueScan(RangeScan& scan);

    EPBucket& bucket;
};