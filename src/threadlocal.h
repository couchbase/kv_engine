/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2014 Couchbase, Inc
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

#ifndef SRC_THREADLOCAL_H_
#define SRC_THREADLOCAL_H_ 1

#include <pthread.h>
#include <cstdlib>
#include <sstream>
#include <cstring>
#include <stdexcept>

#define MAX_THREADS 100

/**
 * Container of thread-local data.
 */
template<typename T>
class ThreadLocal {
public:
    ThreadLocal() {
        int rc = pthread_key_create(&key, NULL);
        if (rc != 0) {
            fprintf(stderr, "Failed to create a thread-specific key: %s\n", strerror(rc));
            abort();
        }
    }

    ~ThreadLocal() {
        pthread_key_delete(key);
    }

    void set(const T &newValue) {
        int rc = pthread_setspecific(key, newValue);
        if (rc != 0) {
            std::stringstream ss;
            ss << "Failed to store thread specific value: " << strerror(rc);
            throw std::runtime_error(ss.str().c_str());
        }
    }

    T get() const {
        return reinterpret_cast<T>(pthread_getspecific(key));
    }

    void operator =(const T &newValue) {
        set(newValue);
    }

    operator T() const {
        return get();
    }

private:
    pthread_key_t key;
};

/**
 * Container for a thread-local pointer.
 */
template <typename T>
class ThreadLocalPtr : public ThreadLocal<T*> {
public:
    ThreadLocalPtr() : ThreadLocal<T*>() {}

    ~ThreadLocalPtr() {}

    T *operator ->() {
        return ThreadLocal<T*>::get();
    }

    T operator *() {
        return *ThreadLocal<T*>::get();
    }

    void operator =(T *newValue) {
        this->set(newValue);
    }
};

#endif  // SRC_THREADLOCAL_H_
