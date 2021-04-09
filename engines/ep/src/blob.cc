/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2017-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
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
