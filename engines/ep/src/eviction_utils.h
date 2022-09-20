/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2022-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */
#pragma once
/**
 * Collection of eviction-related utilities and constants.
 */

#include <cstdint>
#include <limits>

// Max Value for NRU bits
constexpr uint8_t MAX_NRU_VALUE = 3;
// Initial value for NRU bits
constexpr uint8_t INITIAL_NRU_VALUE = 2;
// Min value for NRU bits
constexpr uint8_t MIN_NRU_VALUE = 0;

namespace cb::eviction {
/**
 * The number of low bits in a HLC cas which are _not_ time based.
 *
 * Shifting a cas down by this many bits gives just the timestamp portion
 * (see HLCT)
 */
constexpr uint64_t casBitsNotTime = 16;

// The maximum value that can be added to the age histogram
constexpr uint64_t maxAgeValue =
        std::numeric_limits<uint64_t>::max() >> casBitsNotTime;

/**
 * Map from the 8-bit probabilistic counter (256 states) to NRU (4 states).
 */
uint8_t convertFreqCountToNRUValue(uint8_t statCounter);

/**
 * The initial frequency count that items should be set to when first
 * added to the hash table. It can't generally start at 0, as we want to ensure
 * that we do not immediately evict items that we have just added.
 */
uint8_t getInitialFreqCount();
} // namespace cb::eviction
