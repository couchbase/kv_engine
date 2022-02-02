/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2014-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */
#pragma once

#ifndef SRC_THREADLOCAL_H_
#error "Include threadlocal.h instead"
#endif

#include <pthread.h>
#include <cstdlib>
#include <string>
#include <cstring>
#include <system_error>
#include <iostream>

/**
 * Container of thread-local data.
 */
template<typename T>
class ThreadLocalPosix {
public:
    explicit ThreadLocalPosix(ThreadLocalDestructor dtor = nullptr)
        : dtor(dtor) {
        int rc = pthread_key_create(&key, dtor);
        if (rc != 0) {
            std::string msg = "Failed to create a thread-specific key: ";
            msg.append(strerror(rc));
            throw std::system_error(rc, std::system_category(), msg);
        }
    }

    ~ThreadLocalPosix() {
        // pthread_key_delete doesn't run the destructor so it must be manually
        // invoked if the thread-local has been initialised on this thread.
        void* v = pthread_getspecific(key);
        if (v != nullptr) {
            pthread_setspecific(key, nullptr);
            if (dtor != nullptr) {
                dtor(v);
            }
        }

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

    explicit operator T() const {
        return get();
    }

private:
    pthread_key_t key;
    ThreadLocalDestructor dtor;
};
