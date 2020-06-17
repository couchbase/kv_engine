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
#pragma once

#define SRC_THREADLOCAL_H_ 1

// thread local variable dtor
using ThreadLocalDestructor = void (*)(void*);

#ifdef WIN32
#include "threadlocal_win32.h"
template<typename T>
using ThreadLocal = ThreadLocalWin32<T>;
#else
#include "threadlocal_posix.h"
template<typename T>
using ThreadLocal = ThreadLocalPosix<T>;
#endif

/**
 * Container for a thread-local pointer.
 */
template <typename T>
class ThreadLocalPtr : public ThreadLocal<T*> {
public:
    explicit ThreadLocalPtr(ThreadLocalDestructor dtor = nullptr)
        : ThreadLocal<T*>(dtor) {
    }

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
