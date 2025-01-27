/*
 *     Copyright 2025-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */

#pragma once
#include "steppable_command_context.h"

namespace cb {
class AwaitableSemaphore;
}
enum class TaskId;
class GlobalTask;
using ExTask = std::shared_ptr<GlobalTask>;

/**
 * The BackgroundThreadCommandContext is a state machine used by the memcached
 * core to implement operations which require a background thread to perform
 * the operation.
 */
class BackgroundThreadCommandContext : public SteppableCommandContext {
public:
    /**
     * Create a new BackgroundThreadCommandContext
     *
     * @param cookie the command context
     * @param id The task identifier for the task
     * @param name The name for the task
     * @param semaphore The semaphore to use concurrency limiting
     * @param expectedRuntime The expected runtime for the task
     */
    BackgroundThreadCommandContext(Cookie& cookie,
                                   TaskId id,
                                   std::string name,
                                   cb::AwaitableSemaphore& semaphore,
                                   std::chrono::microseconds expectedRuntime =
                                           std::chrono::milliseconds(100));

protected:
    /**
     * The operation to be executed on the background thread. The return
     * value of this function is stored in the status member variable.
     * and the cookie will be notified with a "success" status. The default
     * implementation of "done" sends this status code back to the client.
     * If one wants to add more information to the response, one should
     * override the done method.
     */
    virtual cb::engine_errc execute() = 0;

    /**
     * The operation is done running on the background thread. It tries
     * to send the status code, with the provided response, datatype and
     * cas back to the client. (if the status code is != success it'll
     * set the content of the response in the error context of the cookie).
     */
    virtual cb::engine_errc done();

    /// The status code for the operation
    cb::engine_errc status{cb::engine_errc::success};
    /// The response to send back to the client
    std::string response;
    /// The cas to use in the response
    std::atomic_uint64_t cas{0};
    /// The datatype to use in the response
    cb::mcbp::Datatype datatype{cb::mcbp::Datatype::Raw};

private:
    /// The step method shouldn't be overridden by the subclasses as
    /// the purpose of this class is to remove all that duplication.
    cb::engine_errc step() final;

    /// This is the method being executed on the background thread and
    /// its purpose is to execute the overridden "execute" method and
    /// finally notify the cookie to ensure that the connection object
    /// gets rescheduled.
    void execute_task_and_notify();

    /// The different states in the state machinery
    enum class State {
        /// Schedule the task for execution and await for it to complete
        ScheduleTask,
        /// The task is done and we should return the result to the client
        Done
    };

    /// The task object to run on the background thread
    ExTask task;

    /// The current state of the state machinery
    State state = State::ScheduleTask;
};
