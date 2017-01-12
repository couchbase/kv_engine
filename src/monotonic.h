/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2017 Couchbase, Inc
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

/**
 * Monotonic is a class template for simple types T. It provides guarentee
 * of updating the class objects by only values that are greater than what is
 * contained at the time of update
 * Note: This is not atomic/thread-safe
 */
template <typename T>
class Monotonic {
public:
    Monotonic(const T val = std::numeric_limits<T>::min()) : val(val) {
    }

    Monotonic(const Monotonic<T>& other) : val(other.val) {
    }

    Monotonic& operator=(const Monotonic<T>& other) {
        if (val < other.val) {
            val = other.val;
        }
        return *this;
    }

    Monotonic& operator=(const T& v) {
        if (val < v) {
            val = v;
        }
        return *this;
    }

    operator T() const {
        return val;
    }

private:
    T val;
};
