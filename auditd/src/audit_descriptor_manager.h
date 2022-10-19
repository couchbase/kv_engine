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
 * audit events in the system (and if they're enabled or not).
 *
 * Note that there is a small window around audit reconfiguration
 * where we'll be using a mix of the old and new configuration (which
 * means that audit reconfiguration is not atomic).
 *
 * The reason is that the audit reconfigure event gets put in
 * the same queue as other events which needs to be put to disk,
 * but the front end threads will continue to use the old filter
 * for all requests put to the system _until_ the reconfiguration
 * happens. Given that this is a short window (from the configuration
 * event was received and until we've processed all queued events
 * and completed the reconfigure) we chose to do that for being
 * able to not serialize everything.
 */
class AuditDescriptorManager {
public:
    /// Get the one and only instance
    static AuditDescriptorManager& instance();

    /// Look up the event descriptor for the id or nullptr if not found
    const EventDescriptor* lookup(uint32_t id) const;

    /// Iterate over all the known identifiers and call the provided callback
    void iterate(std::function<void(EventDescriptor&)> callback);

private:
    AuditDescriptorManager();
    std::unordered_map<uint32_t, EventDescriptor> descriptors;
};
