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

#include "eventdescriptor.h"
#include <functional>
#include <unordered_map>

/**
 * The audit descriptor manager contains the knowledge of all
 * audit events in the system. It is only processes with the
 * audit privilege which may submit events, and the privilege
 * is only given to couchbase server processes part of the TCB
 * which means that we should never operate on unknown
 * identifiers (that would be a bug in those processes).
 */
class AuditDescriptorManager {
public:
    /// Look up the event descriptor for the id.
    /// @throws std::out_of_range for unknown identifiers
    static const EventDescriptor& lookup(uint32_t id);

    /// Iterate over all available descriptors
    /// @todo remove this once we add support for specifying the audit filter
    ///       in the audit configuration
    static void iterate(std::function<void(const EventDescriptor&)> callback);

private:
    AuditDescriptorManager();
    static AuditDescriptorManager& instance();
    const std::unordered_map<uint32_t, EventDescriptor> descriptors;
};
