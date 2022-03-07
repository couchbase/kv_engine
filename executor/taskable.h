/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2015-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */

/*
    An abstract baseclass for classes which wish to create and own tasks.
*/

#pragma once

#include "globaltask.h"
#include "workload.h"

#include <chrono>

/*
    A type for identifying all tasks belonging to a task owner.
*/
typedef uintptr_t task_gid_t;

class Taskable {
public:
    /*
        Return a name for the task, used for logging
    */
    virtual const std::string& getName() const = 0;

    /*
        Return a 'group' ID for the task.

        The use-case here is to allow the lookup of all tasks
        owned by this taskable.

        The address of the object is a safe GID.
    */
    virtual task_gid_t getGID() const = 0;

    /*
        Return the workload priority for the task
    */
    virtual bucket_priority_t getWorkloadPriority() const = 0;

    /*
        Set the taskable object's workload priority.
    */
    virtual void setWorkloadPriority(bucket_priority_t prio) = 0;

    /*
        Return the taskable object's workload policy.
    */
    virtual WorkLoadPolicy& getWorkLoadPolicy() = 0;

    /**
     * Log the the time the given task spent queued (ready to run but not yet
     * started to execute).
     * @param task The task which is about to run
     * @param threadName The name of the thread the task is about to run on.
     * @param runtime Duration the task was queued for.
     */
    virtual void logQTime(const GlobalTask& task,
                          std::string_view threadName,
                          std::chrono::steady_clock::duration enqTime) = 0;

    /**
     * Log the the time the given task spent running.
     * @param task The task which just ran.
     * @param threadName The name of the thread the task ran on.
     * @param runtime Duration the task ran for.
     */
    virtual void logRunTime(const GlobalTask& task,
                            std::string_view threadName,
                            std::chrono::steady_clock::duration runTime) = 0;

    /// @returns True if the Taskable is (in the process of) shutting down.
    virtual bool isShutdown() const = 0;

protected:
    virtual ~Taskable() = default;
};
