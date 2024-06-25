/*
 *     Copyright 2024-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */

#pragma once

#include "executor/task_type.h"
#include <folly/Synchronized.h>
#include <folly/ThreadLocal.h>
#include <utilities/testing_hook.h>
#include <chrono>

/**
 * A record of a pending IO request which has started at some point in the past.
 */
struct FileOp {
    /// The chrono::time_point specialisation used by the clock.
    using TimePoint = std::chrono::steady_clock::time_point;
    /// The type of IO request.
    enum class Type {
        /// No IO is being performed.
        None,
        /// Reading, seeking, copying, etc.
        Read,
        /// Writing, truncating, preallocating, etc.
        Write,
        /// Explicit calls to fsync/fdatasync.
        Sync,
        /// Opening, including via mmap.
        Open,
        /// Closing, including via munmap.
        Close,
    };

    explicit FileOp(Type type = Type::None);
    FileOp(Type type, size_t nbytes);

    static FileOp open() {
        return FileOp{Type::Open};
    }
    static FileOp close() {
        return FileOp{Type::Close};
    }
    static FileOp sync() {
        return FileOp{Type::Sync};
    }
    static FileOp read(size_t nbytes) {
        return FileOp{Type::Read, nbytes};
    }
    static FileOp write(size_t nbytes) {
        return FileOp{Type::Write, nbytes};
    }

    FileOp(const FileOp&) = default;
    FileOp& operator=(const FileOp&) = default;

    /**
     * Returns true if the operation should be treated as a data write operation
     * (as opposed to a data read operation).
     */
    bool isDataWrite() const;

    /// The type of this pending request.
    Type type{Type::None};
    /// The approximate size of the request. Set to 0 bytes, if the size of the
    /// request cannot be determined (open/close/unlink). For sync requests, the
    /// value is set by the tracker and is determined to be the cumulative
    /// nbytes since the last sync.
    size_t nbytes{0};
    /// The time at which this request is considered to have started.
    TimePoint startTime{};
};

std::string to_string(FileOp::Type opType);

class FileOpsTracker;

class FileOpsTrackerScopeGuard {
public:
    FileOpsTrackerScopeGuard(FileOpsTracker& tracker);
    ~FileOpsTrackerScopeGuard();

    FileOpsTrackerScopeGuard(const FileOpsTrackerScopeGuard&) = delete;
    FileOpsTrackerScopeGuard(FileOpsTrackerScopeGuard&& other);

    FileOpsTrackerScopeGuard& operator=(const FileOpsTrackerScopeGuard&) =
            delete;
    FileOpsTrackerScopeGuard& operator=(FileOpsTrackerScopeGuard&& other);

    FileOpsTracker* tracker;
};

/**
 * Tracks pending IO requests on associated threads and allows the user to
 * inspect these threads.
 *
 * Internally, it maintains a list of ThreadSlot objects, which are created by
 * their respective threads. The ThreadSlot objects are initialised lazily when
 * the thread first uses the FileOpsTracker and added to a list. On thread exit,
 * the ThreadSlot object is destroyed and removes itself from the list.
 *
 * The ThreadSlots have shared ownership (shared_ptr), to avoid an exiting
 * thread from causing a read after deallocation on the observer thread.
 *
 * Each thread slot contains the thread name, type and the current IO status
 * which is an FileOp object.
 *
 * Currently, async IO is not supported and the FileOpsTracker does not expect
 * to an FileOp starting before the previous one has completed (on the same
 * thread).
 *
 * Usage (on write path):
 *   auto trackWrite = FileOpsTracker::instance()
 *      .startWithScopeGuard(FileOp::Type::Write);
 *   file.write(...);
 *
 * Usage (on check path):
 *   FileOpsTracker::instance()
 *      .visitThreads([](...){
 *          // Look for stuck requests.
 *   });
 */
class FileOpsTracker {
public:
    struct ThreadSlot;

    static FileOpsTracker& instance();

    FileOpsTracker();
    ~FileOpsTracker();

    FileOpsTracker(const FileOpsTracker&) = delete;
    FileOpsTracker(FileOpsTracker&&) = delete;

    FileOpsTracker& operator=(const FileOpsTracker&) = delete;
    FileOpsTracker& operator=(FileOpsTracker&&) = delete;

    /// Track an IO request starting on the calling thread.
    void start(FileOp op);

    /// Track an IO request starting on the calling thread and return a scope
    /// guard which completes it on exit.
    FileOpsTrackerScopeGuard startWithScopeGuard(const FileOp& op);

    /// Track the completion of an IO request on the calling thread.
    void complete();

    using Visitor = std::function<void(TaskType threadType,
                                       std::string_view threadName,
                                       const FileOp& pending)>;

    /// Visits tracked threads with pending IO (threads with no pending IO are
    /// not visited).
    void visitThreads(Visitor visitor);

    /// For testing only.
    TestingHook<const FileOp&> startHook{nullptr};

private:
    /// Obtains the ThreadSlot with the correct NoArena guard.
    ThreadSlot& getThreadSlot();

    /// Lazily initialises and returns the slot for the calling thread.
    /// Note folly::ThreadLocal uses a global array which may be reallocated.
    /// Therefore, it is important to only use it behind a NoArena guard.
    std::unique_ptr<folly::ThreadLocal<ThreadSlot, FileOpsTracker>> threadSlot;
};
