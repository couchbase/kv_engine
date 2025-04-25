#pragma once

#include "durability/passive_durability_monitor.h"
// Needed to access the state
#include "durability/durability_monitor_impl.h"

class PassiveDurabilityMonitorIntrospector {
public:
    static const std::optional<uint64_t> public_getLastReceivedSnapshotHPS(
            const PassiveDurabilityMonitor& pdm) {
        auto s = pdm.state.rlock();
        if (s->receivedSnapshotEnds.empty()) {
            return {};
        }

        return s->receivedSnapshotEnds.back().hps;
    }
};
