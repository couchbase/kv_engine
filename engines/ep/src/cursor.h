/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2018 Couchbase, Inc
 *
 *   Licensed under the Apache License, Version 2.0 (the "License");
 *   you may not use this file except in compliance with the License.
 *   You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 *   Unless required by applicable law or agreed to in writing, software
 *   distributed under the License is distributed on an "AS IS" BASIS,
 *   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *   See the License for the specific language governing permissions and
 *   limitations under the License.
 */

#pragma once

#include <platform/rwlock.h>

#include <memory>
#include <mutex>
#include <shared_mutex>

class CheckpointCursor;

/**
 * Cursor is the object that clients of CheckointManager use to obtain items
 *
 * Cursor safely manages the underlying weak_ptr<CheckpointCursor>.
 * For example DCP active_stream stats maybe trying to lock() the weak_ptr
 * (see getItemsRemaining) whilst a backfill thread is concurrently registering
 * a cursor (which in turn assigns to the weak_ptr)
 */
class Cursor {
public:
#if defined(__MACH__) && __clang_major__ > 7 || !defined(__MACH__)
    using LockType = std::shared_timed_mutex;
#else
    using LockType = cb::RWLock;
#endif

    Cursor() {
    }

    Cursor(std::shared_ptr<CheckpointCursor> cursor) : cursor(cursor) {
    }

    Cursor(const Cursor& in) {
        // Need read access to them
        std::unique_lock<LockType> writer(cursorLock);
        cursor = in.cursor;
    }

    Cursor& operator=(const Cursor& in) {
        // Need exclusive access to us, but read access to them
        std::unique_lock<LockType> writer(cursorLock);
        std::shared_lock<LockType> reader(in.cursorLock);
        cursor = in.cursor;
        return *this;
    }

    Cursor(Cursor&& in) {
        // Need exclusive access to them
        std::unique_lock<LockType> writer1(in.cursorLock);
        cursor = std::move(in.cursor);
    }

    Cursor& operator=(Cursor&& in) {
        // Need exclusive access to both us and them
        std::unique_lock<LockType> writer1(cursorLock);
        std::unique_lock<LockType> writer2(in.cursorLock);
        cursor = std::move(in.cursor);
        return *this;
    }

    void setCursor(std::shared_ptr<CheckpointCursor> cursor) {
        std::unique_lock<LockType> writer(cursorLock);
        this->cursor = cursor;
    }

    std::shared_ptr<CheckpointCursor> lock() const {
        std::shared_lock<LockType> reader(cursorLock);
        return cursor.lock();
    }

    void reset() {
        std::unique_lock<LockType> writer(cursorLock);
        cursor.reset();
    }

private:
    mutable LockType cursorLock;
    std::weak_ptr<CheckpointCursor> cursor;
};

// registering a cursor returns a CursorRegResult
struct CursorRegResult {
    bool tryBackfill; // The requested seqno couldn't be found
    uint64_t seqno; // The seqno found
    Cursor cursor; // The returned cursor
};
