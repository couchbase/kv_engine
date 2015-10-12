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

#ifndef SRC_THREADLOCAL_POSIX_H_
#define SRC_THREADLOCAL_POSIX_H_ 1

#ifndef SRC_THREADLOCAL_H_
#error "Include threadlocal.h instead"
#endif

#include <pthread.h>
#include <cstdlib>
#include <string>
#include <cstring>
#include <system_error>
#include <iostream>

#define MAX_THREADS 500

/**
 * Container of thread-local data.
 */
template<typename T>
class ThreadLocalPosix {
public:
    ThreadLocalPosix() {
        int rc = pthread_key_create(&key, NULL);
        if (rc != 0) {
            std::string msg = "Failed to create a thread-specific key: ";
            msg.append(strerror(rc));
            throw std::system_error(rc, std::system_category(), msg);
        }
    }

    ~ThreadLocalPosix() {
        int rc = pthread_key_delete(key);
        if (rc != 0) {
            std::cerr << "~ThreadLocalPosix() - pthread_key_delete(): "
                      << strerror(rc) << std::endl;
            std::cerr.flush();
        }
    }

    void set(const T &newValue) {
        int rc = pthread_setspecific(key, newValue);
        if (rc != 0) {
            std::string msg("Failed to store thread specific value: ");
            msg.append(strerror(rc));
            throw std::system_error(rc, std::system_category(), msg);
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

#endif  // SRC_THREADLOCAL_POSIX_H_
