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

#include <string>

#pragma once

struct DocKey;

namespace Collections {
namespace VB {

/**
 * The ScanContext holds important data required when visits all of the keys of
 * a vbucket (as part of compaction or a DCP backfill).
 *
 * When scanning historical data, the separator may not be the same value as the
 * current separator, the ScanContext provides methods so that collections can
 * correctly look at a historical key and break it into collection & key using
 * the separator in force for that historical key.
 */
class ScanContext {
public:
    ScanContext() : separator(DefaultSeparator) {
    }

    /**
     * Manage the context's separator. If the DocKey is a system event for a
     * changed separator, then we will update the context and return true.
     *
     * @return true if the separator was updated
     */
    bool manageSeparator(const ::DocKey& key);

    const std::string& getSeparator() const {
        return separator;
    }

private:
    std::string separator;
};
} // end VB
} // end Collections