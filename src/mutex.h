/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2010 Couchbase, Inc
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

#ifndef SRC_MUTEX_H_
#define SRC_MUTEX_H_ 1

#include "config.h"
#include "common.h"

/**
 * Abstraction built on top of pthread mutexes
 */
class Mutex {
public:
    Mutex();

    virtual ~Mutex();

    /**
     * True if I own this lock.
     *
     * Use this only for assertions.
     */
    bool ownsLock(void) const {
        return held && cb_thread_equal(holder, cb_thread_self());
    }

protected:

    // The holders of locks twiddle these flags.
    friend class LockHolder;
    friend class MultiLockHolder;

    void acquire(void);
    void release(void);

    void setHolder(bool isHeld) {
        held = isHeld;
        holder = cb_thread_self();
    }

    cb_mutex_t mutex;
    cb_thread_t holder;
    bool held;

private:
    DISALLOW_COPY_AND_ASSIGN(Mutex);
};

#endif  // SRC_MUTEX_H_
