/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2018-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */

#pragma once

#include <platform/non_negative_counter.h>
#include <string>

namespace Collections::VB {
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

    /**
     * Build from a buffer containing a LEB 128 encoded data
     * @param buf pointer to start of data
     * @param size data size in bytes
     */
    PersistedStats(const char* buf, size_t size);

    /**
     * Build from a buffer containing a LEB 128 encoded data and supply a value
     * to use for the disk-size if it disk-size isn't found in the LEB128 data.
     * @param buf pointer to start of data
     * @param size data size in bytes
     * @param diskSize a value to use if diskSize isn't found in the data
     */
    PersistedStats(const char* buf, size_t size, size_t diskSize);

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

    cb::NonNegativeCounter<uint64_t> itemCount;
    cb::NonNegativeCounter<uint64_t> highSeqno;
    cb::NonNegativeCounter<uint64_t> diskSize;
};

std::ostream& operator<<(std::ostream& os, const PersistedStats& ps);

} // namespace Collections::VB
