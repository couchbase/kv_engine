/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2011 Couchbase, Inc.
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
#include "config.h"
#include "mutex.h"
#include "common.h"

Mutex::Mutex() : held(false)
{
    pthread_mutexattr_t *attr = NULL;
    int e=0;

#ifdef HAVE_PTHREAD_MUTEX_ERRORCHECK
    pthread_mutexattr_t the_attr;
#ifdef VALGRIND
    // valgrind complains about an uninitialzed memory read
    // if we just initialize the mutexattr with pthread_mutexattr_init.
    memset(&the_attr, 0, sizeof(the_attr));
#endif
    attr = &the_attr;

    if (pthread_mutexattr_init(attr) != 0 ||
        (e = pthread_mutexattr_settype(attr, PTHREAD_MUTEX_ERRORCHECK)) != 0) {
        std::string message = "MUTEX ERROR: Failed to initialize mutex: ";
        message.append(std::strerror(e));
        throw std::runtime_error(message);
    }
#endif

#ifdef VALGRIND
    // valgrind complains about an uninitialzed memory read
    // if we just initialize the mutex with pthread_mutex_init.
    memset(&mutex, 0, sizeof(mutex));
#endif

    if ((e = pthread_mutex_init(&mutex, attr)) != 0) {
        std::string message = "MUTEX ERROR: Failed to initialize mutex: ";
        message.append(std::strerror(e));
        throw std::runtime_error(message);
    }
    EP_MUTEX_CREATED(this);
}

Mutex::~Mutex() {
    int e;
    if ((e = pthread_mutex_destroy(&mutex)) != 0) {
        if (e == EINVAL) {
            std::string err = std::strerror(e);
            // mutex might have already destroyed, just log error
            // and continue.  TODO: platform specific error handling
            // for the case of EINVAL, especially on WIN32
            getLogger()->log(EXTENSION_LOG_WARNING, NULL,
                             "Warning: Failed to destroy mutex: %s\n",
                             err.c_str());
        } else {
            std::string message = "MUTEX ERROR: Failed to destroy mutex: ";
            message.append(std::strerror(e));
            throw std::runtime_error(message);
        }
    }

    EP_MUTEX_DESTROYED(this);
}

void Mutex::acquire() {
    int e;
    if ((e = pthread_mutex_lock(&mutex)) != 0) {
        std::cerr << "MUTEX ERROR: Failed to acquire lock: ";
        std::cerr << std::strerror(e) << std::endl;
        std::cerr.flush();
        abort();
    }
    setHolder(true);

    EP_MUTEX_ACQUIRED(this);
}

void Mutex::release() {
    assert(held && pthread_equal(holder, pthread_self()));
    setHolder(false);
    int e;
    if ((e = pthread_mutex_unlock(&mutex)) != 0) {
        std::cerr << "MUTEX ERROR: Failed to release lock: ";
        std::cerr << std::strerror(e) << std::endl;
        std::cerr.flush();
        abort();
    }
    EP_MUTEX_RELEASED(this);
}

