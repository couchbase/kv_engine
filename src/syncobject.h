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

#include "common.h"

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

    void wait(const struct timeval &tv) {
        // Todo:
        //   This logic is a bit weird, because normally we want to
        //   sleep for a certain amount of time, but since we built
        //   the stuff on pthreads and the API looked like it did we
        //   used the absolute timers making us sleep to a certain
        //   point in the future.. now we need to roll back that work
        //   and do it again in the library API...
        //   I believe we should rather try to modify our own code
        //   to only do relative waits, and then have the native
        //   calls do the either absolute or relative checks.
        //
        //   There is no point of having an explicit return
        //   value if it was a timeout or something else, because
        //   you would have to evaluate the reason you waited anyway
        //   (because one could have spurious wakeups etc)
        struct timeval now;
        gettimeofday(&now, NULL);

        if (tv.tv_sec < now.tv_sec) {
            return ;
        }

        int msec = (tv.tv_sec - now.tv_sec) * 1000;
        int add = 0;
        if (tv.tv_usec < now.tv_usec) {
            msec -= 1000;
            if (msec < 0) {
                return ;
            }
            add = 1000000;
        }
        msec += (tv.tv_usec + add - now.tv_usec) / 1000;
        cb_cond_timedwait(&cond, &mutex, msec);
        setHolder(true);
    }

    void wait(const double secs) {
        cb_cond_timedwait(&cond, &mutex, (unsigned int)(secs * 1000.0));
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
