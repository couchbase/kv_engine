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

#include <executor/taskable.h>
#include <folly/portability/GMock.h>

/**
 * Mock task owner for testing purposes.
 */
class MockTaskable : public Taskable {
public:
    MockTaskable(bucket_priority_t priority = HIGH_BUCKET_PRIORITY);

    const std::string& getName() const override;

    task_gid_t getGID() const override;

    bucket_priority_t getWorkloadPriority() const override;

    void setWorkloadPriority(bucket_priority_t prio) override;

    WorkLoadPolicy& getWorkLoadPolicy() override;

    MOCK_METHOD(void,
                logQTime,
                (const GlobalTask& task,
                 std::string_view threadName,
                 std::chrono::steady_clock::duration enqTime),
                (override));

    void logRunTime(const GlobalTask& task,
                    std::string_view threadName,
                    std::chrono::steady_clock::duration runTime) override;

    bool isShutdown() override;

    void setName(std::string name);

protected:
    std::string name;
    WorkLoadPolicy policy;
};
