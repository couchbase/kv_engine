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

#include <functional>
#include <string>
#include <string_view>
#include <vector>

namespace cb::ioctl {

enum class Id {
    JemallocProfActive,
    JemallocProfDump,
    ReleaseFreeMemory,
    ServerlessMaxConnectionsPerBucket,
    ServerlessReadUnitSize,
    ServerlessWriteUnitSize,
    Sla,
    TraceConfig,
    TraceDumpBegin,
    TraceDumpClear,
    TraceDumpGet,
    TraceDumpList,
    TraceStart,
    TraceStatus,
    TraceStop,
    enum_max
};

enum class Mode { RDONLY, WRONLY, RW };

/// The information we keep for an IO control
class Control {
public:
    /// The identifier for the io control
    const Id id;
    /// The key used to identify the io control
    const std::string_view key;
    /// A "longer" description of the io control
    const std::string_view description;
    /// The mode for the given key
    const Mode mode;
};

/// The manager contains knowledge about all io controls in the system.
class Manager {
public:
    /// Get the one and only instance of the Manager
    static const Manager& getInstance();

    /**
     * Try to look up the io control matching the key
     *
     * @param key the io control to look up
     * @return The io control if found, nullptr for unknown keys
     */
    const Control* lookup(std::string_view key) const;

    /**
     * Look up the io control for the provided id
     *
     * @param id the io control to look up
     * @return The io control
     */
    const Control& lookup(Id id) const;

    /// Iterate over all the available controls and call the provided callback
    void iterate(std::function<void(const Control&)> callback) const;

protected:
    Manager();

    const std::vector<Control> entries;
};
} // namespace cb::ioctl
