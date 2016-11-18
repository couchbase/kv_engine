/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2016 Couchbase, Inc.
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

#include <cstddef>
#include <functional>
#include <string>

/* Struct representing a buffer of some known size. This is used to
 * refer to some existing region of memory which is owned elsewhere - i.e.
 * this object does not have ownership semantics.
 * A user should not free() the buf member themselves!
 */
template <typename T>
struct sized_buffer {
    sized_buffer()
        : sized_buffer(nullptr, 0) {}

    sized_buffer(T* buf_, size_t len_)
        : buf(buf_),
          len(len_) {}

    sized_buffer(const std::string& str)
        : buf(str.data()),
          len(str.size()) { }

    T* data() {
        return buf;
    }

    const T* data() const {
        return buf;
    }

    size_t size() const {
        return len;
    }

    T* buf;
    size_t len;
};

using char_buffer = sized_buffer<char>;
using const_char_buffer = sized_buffer<const char>;

/*
 * Specialization of std::hash<> for sized_buffer & const_sized_buffer.
 */
template<typename T>
static size_t hash_array(const T* base, size_t len);

namespace std {
    template<>
    struct hash<char_buffer> {
        size_t operator()(const sized_buffer<char>& s) const {
            return hash_array<char>(s.buf, s.len);
        }
    };

    template<>
    struct hash<const_char_buffer> {
        size_t operator()(const const_char_buffer& s) const {
            return hash_array<char>(s.buf, s.len);
        }
    };
}

/*
 * Simple hash function for a contiguous array of primitive types.
 */
template<typename T>
static size_t hash_array(const T* base, size_t len) {
    size_t rv = 5381;

    for (size_t ii = 0; ii < len; ii++) {
        rv = ((rv << 5) + rv) ^ size_t(base[ii]);
    }
    return rv;
}
