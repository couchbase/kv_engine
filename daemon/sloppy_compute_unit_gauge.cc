/*
 *    Copyright 2022-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */

#include "sloppy_compute_unit_gauge.h"

SloppyComputeUnitGauge::SloppyComputeUnitGauge() {
    for (auto& e : slots) {
        e.store(0);
    }
}

void SloppyComputeUnitGauge::increment(std::size_t used) {
    slots.at(current.load()) += used;
}

bool SloppyComputeUnitGauge::isBelow(std::size_t value) const {
    return slots.at(current.load()) < value;
}

void SloppyComputeUnitGauge::tick() {
    size_t next = current + 1;
    if (next == slots.size()) {
        next = 0;
    }
    slots[next].store(0);
    current = next;
}

void SloppyComputeUnitGauge::iterate(
        std::function<void(std::size_t)> function) const {
    size_t entry = current + 1;

    for (std::size_t ii = 0; ii < slots.size(); ++ii) {
        if (entry == slots.size()) {
            entry = 0;
        }
        function(slots[entry].load());
        ++entry;
    }
}
