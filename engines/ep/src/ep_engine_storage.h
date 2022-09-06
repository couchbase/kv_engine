/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2022-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */

#pragma once

#include <memcached/engine_storage.h>

class EventuallyPersistentEngine;

class EPEngineStorageBase : public cb::EngineStorageIface {
public:
    EPEngineStorageBase();

protected:
    void deallocate() const final;

private:
    EventuallyPersistentEngine* owner;
};

/**
 * A template type implementing the engine storage interface which can store any
 * MoveConstructible T.
 */
template <typename T>
class EPEngineStorage : public EPEngineStorageBase {
public:
    /// Create a new instance initialised to the given value.
    template <typename U,
              typename = std::enable_if_t<std::is_same_v<T, std::decay_t<U>>>>
    explicit EPEngineStorage(U&& data) : data(std::move(data)) {
    }

    /// Access the stored value.
    const T& get() const {
        return data;
    }

    /**
     * Move the stored value out of the engine storage and return it to the
     * caller.
     */
    T take() {
        return std::move(data);
    }

private:
    T data;
};
