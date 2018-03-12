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

#include "collections/scan_context.h"
#include "collections/collections_dockey.h"

namespace Collections {
namespace VB {

bool ScanContext::manageSeparator(const ::DocKey& key) {
    if (key.getDocNamespace() == DocNamespace::System) {
        auto cKey = Collections::DocKey::make(key);
        if (cKey.getCollection() == Collections::SeparatorChangePrefix) {
            // This system event key represents a change to a new separator.
            // The key is formatted as:
            //  - $collections_separator:<uid>:<new_sep>

            // Make a new Collections::DocKey from cKey.getKey, which returns
            // <uid>:<new_sep>, thus getKey will return <new_sep>
            auto cKey2 = Collections::DocKey::make(
                    {reinterpret_cast<const uint8_t*>(cKey.getKey().data()),
                     cKey.getKey().size(),
                     DocNamespace::System});
            separator.assign(cKey2.getKey().data(), cKey2.getKey().size());
            return true; // return true because we've changed
        }
    }
    return false;
}

} // end VB
} // end Collections