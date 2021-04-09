/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2018-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */

#pragma once

#include <folly/SharedMutex.h>
#include <memory>
#include <mutex>
#include <shared_mutex>

class CheckpointCursor;

/**
 * Cursor is the object that clients of CheckpointManager use to obtain items
 *
 * Cursor safely manages the underlying weak_ptr<CheckpointCursor>.
 * For example DCP active_stream stats maybe trying to lock() the weak_ptr
 * (see getItemsRemaining) whilst a backfill thread is concurrently registering
 * a cursor (which in turn assigns to the weak_ptr)
 */
class Cursor {
public:
    using LockType = folly::SharedMutex;

    Cursor() = default;

    explicit Cursor(std::shared_ptr<CheckpointCursor> cursor) : cursor(cursor) {
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
