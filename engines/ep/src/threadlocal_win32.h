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

#include <cstdlib>
#include <cstring>
#include <iosfwd>
#include <iostream>
#include <string>
#include <system_error>

#include <platform/strerror.h>

/**
 * Container of thread-local data.
 */
template<typename T>
class ThreadLocalWin32 {
public:
    ThreadLocalWin32(ThreadLocalDestructor dtor = nullptr) {
        tlsIndex = TlsAlloc();
        if (tlsIndex == TLS_OUT_OF_INDEXES) {
            DWORD err = GetLastError();
            std::string msg("Failed to create thread local storage: ");
            msg.append(cb_strerror(err));
            throw std::system_error(err, std::system_category(), msg);
        }
    }

    ~ThreadLocalWin32() {
        if (!TlsFree(tlsIndex)) {
            std::cerr << "~ThreadLocalPosix() TlsFree: "
                      << cb_strerror() << std::endl;
            std::cerr.flush();
        }
    }

    void set(const T &newValue) {
        if (!TlsSetValue(tlsIndex, newValue)) {
            DWORD err = GetLastError();
            std::string msg("Failed to store thread specific value: ");
            msg.append(cb_strerror(err));
            throw std::system_error(err, std::system_category(), msg);
        }
    }

    T get() const {
        return reinterpret_cast<T>(TlsGetValue(tlsIndex));
    }

    void operator =(const T &newValue) {
        set(newValue);
    }

    operator T() const {
        return get();
    }

private:
    // No use in Win32. Only for compatibility
    ThreadLocalDestructor dtor;
    DWORD tlsIndex;
};
