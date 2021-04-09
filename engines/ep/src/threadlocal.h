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
