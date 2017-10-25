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

#include "collections/collections_types.h"
#include "collections/scan_context.h"
#include "storeddockey.h"

#include <vector>

class VBucket;

#pragma once

namespace Collections {
namespace VB {

/**
 * The EraserContext holds important data that the erasing process requires
 * as it visits all of the keys of a vbucket (as part of compaction).
 * For example it tracks defunct collection separator keys (so the can be
 * purged).
 */
class EraserContext : public ScanContext {
public:
    EraserContext() : staleSeparatorChangeKeys(0), lastSeparatorChangeKey() {
    }

    /**
     * Manage the context's separator. If the DocKey is a system event for a
     * changed separator, then we will update the context and return true.
     *
     * @return true if the separator was updated
     */
    bool manageSeparator(const ::DocKey& key);

    /**
     * Function used to delete the staleSeparatorKeys.
     * @param vb The vbucket to issue deletes against for each key in
     *        staleSeparatorKeys
     */
    void processKeys(VBucket& vb);

private:
    /// A list of separator change keys which can be deleted
    std::vector<StoredDocKey> staleSeparatorChangeKeys;
    StoredDocKey lastSeparatorChangeKey;
};
} // end VB
} // end Collections