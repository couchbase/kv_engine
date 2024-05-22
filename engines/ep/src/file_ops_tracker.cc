/*
 *     Copyright 2024-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */

#include "file_ops_tracker.h"

#include "ep_time.h"
#include "executor/globaltask.h"
#include <fmt/format.h>
#include <folly/system/ThreadName.h>
#include <gsl/gsl-lite.hpp>
#include <platform/cb_arena_malloc.h>
#include <memory>

FileOp::FileOp(Type type) : FileOp(type, 0) {
}

FileOp::FileOp(Type type, size_t nbytes)
    : type(type), nbytes(nbytes), startTime{ep_uptime_now()} {
}

std::string to_string(FileOp::Type type) {
    switch (type) {
    case FileOp::Type::None:
        return "None";
    case FileOp::Type::Read:
        return "Read";
    case FileOp::Type::Write:
        return "Write";
    case FileOp::Type::Sync:
        return "Sync";
    case FileOp::Type::Open:
        return "Open";
    case FileOp::Type::Close:
        return "Close";
    }
    throw std::invalid_argument(
            fmt::format("Invalid FileOp value: {}", static_cast<int>(type)));
}

FileOpsTrackerScopeGuard::FileOpsTrackerScopeGuard(FileOpsTracker& tracker)
    : tracker(&tracker) {
}

FileOpsTrackerScopeGuard::~FileOpsTrackerScopeGuard() {
    if (tracker) {
        tracker->complete();
    }
}

FileOpsTrackerScopeGuard::FileOpsTrackerScopeGuard(
        FileOpsTrackerScopeGuard&& other)
    : tracker(std::exchange(other.tracker, nullptr)) {
}

FileOpsTrackerScopeGuard& FileOpsTrackerScopeGuard::operator=(
        FileOpsTrackerScopeGuard&& other) {
    tracker = std::exchange(other.tracker, nullptr);
    return *this;
}

/**
 * Each tracked thread will have one of these objects.
 * This object contains the currently executing FileOp and the type of thread
 * that it is owned by. Together, these two values are sufficient to filter
 * interesting threads and identify when one of those threads has become stuck.
 */
struct FileOpsTracker::ThreadSlot {
    /// Initialises the slot with current state against the global arena.
    ThreadSlot();
    /// Deiniialises the slot against the global arena.
    ~ThreadSlot();

    /// The type of thread pool this thread belongs to.
    const task_type_t threadType;
    /// The name of the thread. Effectively const and allocated under NoArena.
    std::string threadName;
    /**
     * The request currently pending on the thread.
     * There will almost never be contention here, so use a regular mutex.
     */
    folly::Synchronized<FileOp, std::mutex> currentRequest;
};

FileOpsTracker::ThreadSlot::ThreadSlot()
    : threadType(GlobalTask::getCurrentTaskType()),
      threadName(),
      currentRequest(FileOp{FileOp::Type::None}) {
    cb::NoArenaGuard guard;
    threadName = folly::getCurrentThreadName().value_or("unknown");
}

FileOpsTracker::ThreadSlot::~ThreadSlot() {
    cb::NoArenaGuard guard;
    std::exchange(threadName, {});
}

FileOpsTracker& FileOpsTracker::instance() {
    static FileOpsTracker tracker;
    return tracker;
}

FileOpsTracker::FileOpsTracker() {
    cb::NoArenaGuard guard;
    threadSlot =
            std::make_unique<folly::ThreadLocal<ThreadSlot, FileOpsTracker>>();
}

FileOpsTracker::~FileOpsTracker() {
    cb::NoArenaGuard guard;
    threadSlot.reset();
}

FileOpsTracker::ThreadSlot& FileOpsTracker::getThreadSlot() {
    cb::NoArenaGuard guard;
    return **threadSlot;
}

void FileOpsTracker::start(const FileOp& op) {
    Expects(op.type != FileOp::Type::None);
    if (startHook) {
        startHook(op);
    }
    getThreadSlot().currentRequest = op;
}

FileOpsTrackerScopeGuard FileOpsTracker::startWithScopeGuard(const FileOp& op) {
    start(op);
    return {*this};
}

void FileOpsTracker::complete() {
    getThreadSlot().currentRequest = FileOp{FileOp::Type::None};
}

void FileOpsTracker::visitThreads(
        std::function<void(task_type_t, std::string_view, const FileOp&)>
                visitor) {
    for (auto&& slot : threadSlot->accessAllThreads()) {
        auto op = slot.currentRequest.copy();
        if (op.type != FileOp::Type::None) {
            visitor(slot.threadType, slot.threadName, op);
        }
    }
}
