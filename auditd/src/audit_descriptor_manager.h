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
#include <unordered_map>

/**
 * The audit descriptor manager contains the knowledge of all
 * audit events in the system.
 */
class AuditDescriptorManager {
public:
    /// Get the one and only instance
    static AuditDescriptorManager& instance();

    /// Look up the event descriptor for the id or nullptr if not found
    const EventDescriptor* lookup(uint32_t id) const;

private:
    AuditDescriptorManager();
    const std::unordered_map<uint32_t, EventDescriptor> descriptors;
};
