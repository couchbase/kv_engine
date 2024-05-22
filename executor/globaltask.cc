/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2016-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */

#include "globaltask.h"
#include "taskable.h"
#include <fmt/format.h>
#include <utilities/debug_variable.h>
#include <climits>

// These static_asserts previously were in priority_test.cc
static_assert(TaskPriority::VKeyStatBGFetchTask < TaskPriority::FlusherTask,
              "VKeyStatBGFetchTask not less than FlusherTask");

static_assert(TaskPriority::ItemPager < TaskPriority::BackfillManagerTask,
              "ItemPager not less than BackfillManagerTask");

std::atomic<size_t> GlobalTask::task_id_counter(1);

GlobalTask::GlobalTask(Taskable& t,
                       TaskId taskId,
                       double initialSleepTime,
                       bool completeBeforeShutdown)
    : blockShutdown(completeBeforeShutdown),
      state(TASK_RUNNING),
      uid(nextTaskId()),
      taskId(taskId),
      taskable(t),
      totalRuntime(0),
      previousRuntime(0),
      lastStartTime(0) {
    priority = getTaskPriority(taskId);
    snooze(initialSleepTime);
}

GlobalTask::~GlobalTask() = default;

bool GlobalTask::execute(std::string_view threadName) {
    using namespace std::chrono;
    if (isdead()) {
        return false;
    }

    // Put the taskable name on the stack so we know which bucket's task was
    // running if we happen to crash.
    cb::DebugVariable taskableName(cb::toCharArrayN<32>(taskable.getName()));

    // Call GlobalTask::run(), noting the result.
    // If true: Read GlobalTask::wakeTime. If "now", then re-queue
    // directly on the CPUThreadPool. If some time in the future,
    // then schedule on the IOThreadPool for the given time.
    // If false: Cancel task, will not run again.

    const auto executedAt = steady_clock::now();

    // Calculate and record scheduler overhead.
    auto scheduleOverhead = executedAt - getWaketime();
    // scheduleOverhead can be a negative number if the task has
    // been woken up before we expected it too be. In this case this
    // means that we have no schedule overhead and thus need to set
    // it too 0.
    if (scheduleOverhead < steady_clock::duration::zero()) {
        scheduleOverhead = steady_clock::duration::zero();
    }

    getTaskable().logQTime(*this, threadName, scheduleOverhead);

    const auto start = steady_clock::now();
    updateLastStartTime(start);

    setState(TASK_RUNNING, TASK_SNOOZED);
    bool runAgain = run();

    const auto end = steady_clock::now();
    auto runtime = end - start;
    getTaskable().logRunTime(*this, threadName, runtime);
    updateRuntime(runtime);

    return runAgain;
}

void GlobalTask::snooze(const double secs) {
    if (secs == INT_MAX) {
        setState(TASK_SNOOZED, TASK_RUNNING);
        updateWaketime(std::chrono::steady_clock::time_point::max());
        return;
    }

    const auto curTime = std::chrono::steady_clock::now();
    if (secs) {
        setState(TASK_SNOOZED, TASK_RUNNING);
        auto nano_secs = static_cast<int64_t>(secs * 1000000000);
        updateWaketime(curTime + std::chrono::nanoseconds(nano_secs));
    } else {
        updateWaketime(curTime);
    }
}

void GlobalTask::wakeUp() {
    updateWaketime(std::chrono::steady_clock::now());
}

/*
 * Generate a switch statement from tasks.def.h that maps TaskId to a
 * stringified value of the task's name.
 */
const char* GlobalTask::getTaskName(TaskId id) {
    switch (id) {
#define TASK(name, type, prio) \
    case TaskId::name: {       \
        return #name;          \
    }
#include "tasks.def.h"
#undef TASK
    case TaskId::TASK_COUNT: {
        throw std::invalid_argument(
                "GlobalTask::getTaskName(TaskId::TASK_COUNT) called.");
    }
    }
    throw std::logic_error("GlobalTask::getTaskName() unknown id " +
                           std::to_string(static_cast<int>(id)));
    return nullptr;
}

/*
 * Generate a switch statement from tasks.def.h that maps TaskId to priority
 */
TaskPriority GlobalTask::getTaskPriority(TaskId id) {
    switch (id) {
#define TASK(name, type, prio)     \
    case TaskId::name: {           \
        return TaskPriority::name; \
    }
#include "tasks.def.h"
#undef TASK
    case TaskId::TASK_COUNT: {
        throw std::invalid_argument(
                "GlobalTask::getTaskPriority(TaskId::TASK_COUNT) called.");
    }
    }
    throw std::logic_error("GlobalTask::getTaskPriority() unknown id " +
                           std::to_string(static_cast<int>(id)));
    return TaskPriority::PRIORITY_COUNT;
}

/*
 * Generate a switch statement from tasks.def.h that maps TaskId to task type
 */
TaskType GlobalTask::getTaskType(TaskId id) {
    switch (id) {
#define TASK(name, type, prio) \
    case TaskId::name: {       \
        return type;           \
    }
#include "tasks.def.h"
#undef TASK
    case TaskId::TASK_COUNT: {
        throw std::invalid_argument(
                "GlobalTask::getTaskType(TaskId::TASK_COUNT) called.");
    }
    }
    throw std::logic_error("GlobalTask::getTaskType() unknown id " +
                           std::to_string(static_cast<int>(id)));
}

std::string GlobalTask::getTaskIdString(TaskId id) {
    return fmt::format("{}[{}]", getTaskName(id), to_string(getTaskType(id)));
}

std::array<TaskId, static_cast<int>(TaskId::TASK_COUNT)>
        GlobalTask::allTaskIds = {{
#define TASK(name, type, prio) TaskId::name,
#include "tasks.def.h"
#undef TASK
        }};

std::string to_string(task_state_t state) {
    switch (state) {
    case TASK_RUNNING:
        return "RUNNING";
    case TASK_SNOOZED:
        return "SNOOZED";
    case TASK_DEAD:
        return "DEAD";
    }
    throw std::invalid_argument("to_string(task_state_t) unknown state " +
                                std::to_string(static_cast<int>(state)));
}

std::ostream& operator<<(std::ostream& os, const task_state_t& state) {
    return os << to_string(state);
}
