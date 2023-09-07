/*
 *     Copyright 2023-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */
#pragma once

namespace cb {

/**
 * Calculate a fraction of an integer, using sufficient precision to represent
 * the addressable virtual memory.
 *
 * @param val An integer value
 * @param frac The fraction of the value to calculate [0.-1.]
 */
template <typename T>
auto fractionOf(T val, double frac) {
    return static_cast<T>(static_cast<double>(val) * frac);
}

} // namespace cb
