/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2019 Couchbase, Inc
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

#include "collections/flush.h"

namespace Collections {
namespace VB {
class Manifest;
}
} // namespace Collections

namespace VB {

/**
 * The VB::Commit class encapsulates data to be passed to KVStore::commit
 * and is then used by KVStore for the update of on-disk data.
 */
class Commit {
public:
    Commit(Collections::VB::Manifest& manifest) : collections(manifest) {
    }

    /// Object for updating the collection's meta-data during commit
    Collections::VB::Flush collections;
};

} // end namespace VB
