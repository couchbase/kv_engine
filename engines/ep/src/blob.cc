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

#include "blob.h"

#include "objectregistry.h"

#include <cstring>

Blob* Blob::New(const char* start, const size_t len) {
    size_t total_len = getAllocationSize(len);
    Blob* t = new (::operator new(total_len)) Blob(start, len);
    return t;
}

Blob* Blob::New(const size_t len) {
    size_t total_len = getAllocationSize(len);
    Blob* t = new (::operator new(total_len)) Blob(len);
    return t;
}

Blob* Blob::Copy(const Blob& other) {
    Blob* t = new (::operator new(Blob::getAllocationSize(other.valueSize())))
            Blob(other);
    return t;
}

Blob::Blob(const char* start, const size_t len)
    : size(static_cast<uint32_t>(len)), age(0) {
    if (start != nullptr) {
        std::memcpy(data, start, len);
#ifdef VALGRIND
    } else {
        memset(data, 0, len);
#endif
    }
    ObjectRegistry::onCreateBlob(this);
}

Blob::Blob(const size_t len) : Blob(nullptr, len) {
}

Blob::Blob(const Blob& other)
    : size(other.size.load()),
      // While this is a copy, it is a new allocation therefore reset age.
      age(0) {
    std::memcpy(data, other.data, other.valueSize());
    ObjectRegistry::onCreateBlob(this);
}

const std::string Blob::to_s() const {
    return std::string(data, valueSize());
}

Blob::~Blob() {
    ObjectRegistry::onDeleteBlob(this);
}
