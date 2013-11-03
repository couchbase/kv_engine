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

#include <sys/time.h>

#include <iostream>
#include <sstream>
#include <stdexcept>
#include <string>

#include "common.h"

/**
 * Abstraction built on top of pthread mutexes
 */
class SyncObject : public Mutex {
public:
    SyncObject() : Mutex() {
#ifdef VALGRIND
        // valgrind complains about an uninitialzed memory read
        // if we just initialize the cond with pthread_cond_init.
        memset(&cond, 0, sizeof(cond));
#endif
        if (pthread_cond_init(&cond, NULL) != 0) {
            throw std::runtime_error("MUTEX ERROR: Failed to initialize cond.");
        }
    }

    ~SyncObject() {
        int e;
        if ((e = pthread_cond_destroy(&cond)) != 0) {
            if (e == EINVAL) {
                std::string err = std::strerror(e);
                // cond. object might have already destroyed, just log
                // error and continue.  TODO: platform specific error
                // handling for the case of EINVAL, especially on WIN32
                LOG(EXTENSION_LOG_WARNING,
                    "Warning: Failed to destroy cond. object: %s", err.c_str());
            } else {
                throw std::runtime_error("MUTEX ERROR: Failed to destroy cond.");
            }
        }
    }

    void wait() {
        if (pthread_cond_wait(&cond, &mutex) != 0) {
            throw std::runtime_error("Failed to wait for condition.");
        }
        setHolder(true);
    }

    bool wait(const struct timeval &tv) {
        struct timespec ts;
        ts.tv_sec = tv.tv_sec + 0;
        ts.tv_nsec = tv.tv_usec * 1000;

        switch (pthread_cond_timedwait(&cond, &mutex, &ts)) {
        case 0:
            setHolder(true);
            return true;
        case ETIMEDOUT:
            setHolder(true);
            return false;
        default:
            throw std::runtime_error("Failed timed_wait for condition.");
        }
    }

    bool wait(const double secs) {
        struct timeval tv;
        gettimeofday(&tv, NULL);
        advance_tv(tv, secs);
        return wait(tv);
    }

    void notify() {
        if (pthread_cond_broadcast(&cond) != 0) {
            throw std::runtime_error("Failed to broadcast change.");
        }
    }

    void notifyOne() {
        if (pthread_cond_signal(&cond) != 0) {
            throw std::runtime_error("Failed to signal change.");
        }
    }

private:
    pthread_cond_t cond;

    DISALLOW_COPY_AND_ASSIGN(SyncObject);
};

#endif  // SRC_SYNCOBJECT_H_

