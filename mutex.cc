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
#include "mutex.hh"

Mutex::Mutex() : held(false)
{
    pthread_mutexattr_t *attr = NULL;
    int e=0;

#ifdef HAVE_PTHREAD_MUTEX_ERRORCHECK
    pthread_mutexattr_t the_attr;
    attr = &the_attr;

    if (pthread_mutexattr_init(attr) != 0 ||
        (e = pthread_mutexattr_settype(attr, PTHREAD_MUTEX_ERRORCHECK)) != 0) {
        std::string message = "MUTEX ERROR: Failed to initialize mutex: ";
        message.append(std::strerror(e));
        throw std::runtime_error(message);
    }
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
        std::string message = "MUTEX ERROR: Failed to destroy mutex: ";
        message.append(std::strerror(e));
        throw std::runtime_error(message);
    }

    EP_MUTEX_DESTROYED(this);
}

void Mutex::acquire() {
    int e;
    if ((e = pthread_mutex_lock(&mutex)) != 0) {
        std::string message = "MUTEX ERROR: Failed to acquire lock: ";
        message.append(std::strerror(e));
        throw std::runtime_error(message);
    }
    setHolder(true);

    EP_MUTEX_ACQUIRED(this);
}

void Mutex::release() {
    assert(held && pthread_equal(holder, pthread_self()));
    setHolder(false);
    int e;
    if ((e = pthread_mutex_unlock(&mutex)) != 0) {
        std::string message = "MUTEX_ERROR: Failed to release lock: ";
        message.append(std::strerror(e));
        throw std::runtime_error(message);
    }
    EP_MUTEX_RELEASED(this);
}

