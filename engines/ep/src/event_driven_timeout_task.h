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

#include "ep_types.h"

#include <chrono>
#include <memory>

class GlobalTask;
class Taskable;
class VBucket;

/**
 * EventDrivenTimeoutTask is a wrapper class to manage a task's sleep time
 * in response to calls to update and cancel.
 */
class EventDrivenTimeoutTask : public EventDrivenDurabilityTimeoutIface {
public:
    EventDrivenTimeoutTask(std::shared_ptr<GlobalTask> task);

    ~EventDrivenTimeoutTask() override;

    void updateNextExpiryTime(std::chrono::steady_clock::time_point) override;

    void cancelNextExpiryTime() override;

private:
    size_t taskId;
};