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

#ifndef SRC_THREADLOCAL_WIN32_H_
#define SRC_THREADLOCAL_WIN32_H_ 1

#ifndef SRC_THREADLOCAL_H_
#error "Include threadlocal.h instead"
#endif

#include <cstdlib>
#include <sstream>
#include <cstring>
#include <stdexcept>
#include <iostream>
#include <string>

/**
 * Container of thread-local data.
 */
template<typename T>
class ThreadLocalWin32 {
public:
    ThreadLocalWin32() {
        tlsIndex = TlsAlloc();
        if (tlsIndex == TLS_OUT_OF_INDEXES) {
            std::cerr << "Failed to create thread local storage: " << getErrorString() << std::endl;
            std::cerr.flush();
            abort();
        }
    }

    ~ThreadLocalWin32() {
        if (!TlsFree(tlsIndex)) {
            std::cerr << "Failed to release thread local storage: " << getErrorString() << std::endl;
            std::cerr.flush();
            abort();
        }
    }

    void set(const T &newValue) {
        if (!TlsSetValue(tlsIndex, newValue)) {
            std::stringstream ss;
            ss << "Failed to store thread specific value: " << getErrorString();
            throw std::runtime_error(ss.str().c_str());
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

    static std::string getErrorString(void) {
        std::string ret;
        char* win_msg = NULL;
        DWORD err = GetLastError();
        FormatMessageA(FORMAT_MESSAGE_ALLOCATE_BUFFER |
            FORMAT_MESSAGE_FROM_SYSTEM |
            FORMAT_MESSAGE_IGNORE_INSERTS,
            NULL, err,
            MAKELANGID(LANG_NEUTRAL, SUBLANG_DEFAULT),
            (LPTSTR)&win_msg,
            0, NULL);
        ret.assign(win_msg);
        LocalFree(win_msg);
        return ret;
    }

    DWORD tlsIndex;
};

#endif  // SRC_THREADLOCAL_WIN32_H_
