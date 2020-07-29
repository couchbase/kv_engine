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

#pragma once

#include <string>

namespace Collections {
namespace VB {
/**
 * The collection stats that we persist on disk. Provides encoding and
 * decoding of stats.
 */
struct PersistedStats {
    PersistedStats() : itemCount(0), highSeqno(0), diskSize(0) {
    }

    PersistedStats(uint64_t itemCount, uint64_t highSeqno, uint64_t diskSize)
        : itemCount(itemCount), highSeqno(highSeqno), diskSize(diskSize) {
    }

    /// Build from a buffer containing a LEB 128 encoded string
    PersistedStats(const char* buf, size_t size);

    /**
     * @return a LEB 128 encoded version of these stats ready for
     *         persistence
     */
    std::string getLebEncodedStats() const;

    /**
     * For unit testing, expose only itemCount and highSeqno
     * @return a LEB128 encoded 'array' of the stats mad-hatter stored
     */
    std::string getLebEncodedStatsMadHatter() const;

    uint64_t itemCount;
    uint64_t highSeqno;
    uint64_t diskSize;
};
} // end namespace VB
} // end namespace Collections
