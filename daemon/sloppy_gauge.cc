/*
 *    Copyright 2022-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */

#include "sloppy_gauge.h"

void SloppyGauge::increment(std::size_t used) {
    value += used;
}

bool SloppyGauge::isBelow(std::size_t limit) const {
    return value < limit;
}

void SloppyGauge::tick(size_t max) {
    if (max) {
        do {
            if (value > max) {
                value -= max;
                return;
            } else {
                // try to reset the value to 0, but deal with race
                // incrementing the value
                std::size_t next{value.load()};
                if (value.compare_exchange_strong(next, 0)) {
                    return;
                }
            }
        } while (true);
    } else {
        // Don't roll over anything
        value = 0;
    }
}

void SloppyGauge::reset() {
    value = 0;
}
