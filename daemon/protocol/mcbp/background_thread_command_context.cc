/*
 *     Copyright 2025-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */

#include "background_thread_command_context.h"

#include <daemon/connection.h>
#include <daemon/log_macros.h>
#include <daemon/one_shot_limited_concurrency_task.h>
#include <executor/executorpool.h>

BackgroundThreadCommandContext::BackgroundThreadCommandContext(
        Cookie& cookie,
        TaskId id,
        std::string name,
        cb::AwaitableSemaphore& semaphore,
        std::chrono::microseconds expectedRuntime)
    : SteppableCommandContext(cookie),
      taskId(id),
      taskName(std::move(name)),
      taskSemaphore(semaphore),
      taskRuntime(expectedRuntime) {
    // Reallocate the cookie to ensure that the request buffer is preserved
    // until the task is scheduled (so there won't be any dangling pointers
    // if one tries to query the request object from the task).
    cookie.preserveRequest();
}

ExTask BackgroundThreadCommandContext::makeTask() {
    return std::make_shared<OneShotLimitedConcurrencyTask>(
            taskId,
            taskName,
            [this]() { execute_task_and_notify(); },
            taskSemaphore,
            taskRuntime);
}

cb::engine_errc BackgroundThreadCommandContext::done() {
    if (status != cb::engine_errc::success) {
        if (!response.empty()) {
            cookie.setErrorContext(response);
        }
        return status;
    }

    cookie.sendResponse(status, {}, {}, response, datatype, cas);
    return status;
}

cb::engine_errc BackgroundThreadCommandContext::step() {
    while (true) {
        switch (state) {
        case State::ScheduleTask:
            try {
                task = makeTask();
                ExecutorPool::get()->schedule(task);
                state = State::WaitForCompletion;
                return cb::engine_errc::would_block;
            } catch (const std::bad_alloc&) {
                return cb::engine_errc::no_memory;
            }

        case State::WaitForCompletion:
            std::atomic_thread_fence(std::memory_order_acquire);
            // The engine may legitimately return would_block from
            // execute(). When the cookie is notified, reschedule the
            // task so execute() runs again.
            if (status == cb::engine_errc::would_block) {
                state = State::ScheduleTask;
                continue;
            }
            state = State::Done;
            continue;

        case State::Done:
            return done();
        }

        throw std::logic_error(
                "BackgroundThreadCommandContext::step: Invalid state " +
                std::to_string(static_cast<int>(state)));
    }
}

void BackgroundThreadCommandContext::execute_task_and_notify() {
    try {
        status = execute();
    } catch (const std::exception& e) {
        LOG_ERROR_CTX("Exception occurred in BackgroundThreadCommandContext",
                      {"conn_id", cookie.getConnectionId()},
                      {"description", cookie.getConnection().getDescription()},
                      {"cookie", cookie.to_json()},
                      {"error", e.what()});
        status = cb::engine_errc::disconnect;
    }
    std::atomic_thread_fence(std::memory_order_release);
    if (status == cb::engine_errc::would_block) {
        // The engine has retained the cookie and will notifyIoComplete
        // when the operation is ready to proceed. The next step()
        // (driven by that notification) reschedules the task.
        return;
    }
    cookie.notifyIoComplete(cb::engine_errc::success);
}
