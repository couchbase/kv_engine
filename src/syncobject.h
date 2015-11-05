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

#ifndef SRC_SYNCOBJECT_H_
#define SRC_SYNCOBJECT_H_ 1

#include "config.h"

#include "time.h"
#include "utility.h"

/**
 * Abstraction built on top our own condition variable implemntation
 */
class SyncObject : public Mutex {
public:
    SyncObject() : Mutex() {
        cb_cond_initialize(&cond);
    }

    ~SyncObject() {
        cb_cond_destroy(&cond);
    }

    void wait() {
        cb_cond_wait(&cond, &mutex);
        setHolder(true);
    }

    void wait(const double secs) {
        cb_cond_timedwait(&cond, &mutex, (unsigned int)(secs * 1000.0));
        setHolder(true);
    }

    void wait(const hrtime_t nanoSecs) {
        cb_cond_timedwait(&cond, &mutex, (unsigned int)(nanoSecs/1000000));
        setHolder(true);
    }

    void notify() {
        cb_cond_broadcast(&cond);
    }

    void notifyOne() {
        cb_cond_signal(&cond);
    }

private:
    cb_cond_t cond;

    DISALLOW_COPY_AND_ASSIGN(SyncObject);
};

#endif  // SRC_SYNCOBJECT_H_
