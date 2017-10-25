/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2018 Couchbase, Inc
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

#include "collections/collections_dockey.h"

/**
 * Factory method to create a Collections::DocKey from a DocKey
 */
Collections::DocKey Collections::DocKey::make(const ::DocKey& key) {
    if (key.getDocNamespace() != DocNamespace::System) {
        throw std::invalid_argument("DocKey::make incorrect namespace:" +
                                    std::to_string(int(key.getDocNamespace())));
    }
    const uint8_t* collection = findCollection(key, SystemSeparator);
    if (collection) {
        return DocKey(key, collection - key.data(), 1);
    } else {
        // No collection found, not an error - ok for DefaultNamespace.
        return DocKey(key, 0, 0);
    }
}
