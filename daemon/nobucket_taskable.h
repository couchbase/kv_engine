/*
 *     Copyright 2021-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */
#pragma once

#include <executor/taskable.h>

/**
 * The executor wants a "Taskable" class to be provided. It is not
 * something which may be scheduled to run a task as the name implies
 * but more a "Task owner" or "Task monitor". An entity which owns
 * certain tasks and gets notified with its runtime information etc.
 *
 * The core is its own entity and all of its tasks don't belong to any
 * bucket so have a singleton object representing all of the tasks
 * which don't belong to a bucket
 */
class NoBucketTaskable : public Taskable {
public:
    const std::string& getName() const override;
    task_gid_t getGID() const override;
    bucket_priority_t getWorkloadPriority() const override;
    void setWorkloadPriority(bucket_priority_t prio) override;
    WorkLoadPolicy& getWorkLoadPolicy() override;
    void logQTime(const GlobalTask&,
                  std::string_view,
                  std::chrono::steady_clock::duration) override;

    void logRunTime(const GlobalTask&,
                    std::string_view,
                    std::chrono::steady_clock::duration) override;

    bool isShutdown() override;

    /// Get the one and only instance of the NoBucketTaskable
    static NoBucketTaskable& instance();

protected:
    ~NoBucketTaskable() override = default;
    const std::string name{"No bucket"};
    WorkLoadPolicy policy{0, 0};
};
