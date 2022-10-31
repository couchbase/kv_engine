/*
 *     Copyright 2022-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */

#include "audit_descriptor_manager.h"
#include <stdexcept>

AuditDescriptorManager::AuditDescriptorManager()
    : descriptors(
#include "auditd/audit_descriptor_manager_defs.cc"
      ) {
}

AuditDescriptorManager& AuditDescriptorManager::instance() {
    static AuditDescriptorManager inst;
    return inst;
}

const EventDescriptor& AuditDescriptorManager::lookup(uint32_t id) {
    return instance().descriptors.at(id);
}

void AuditDescriptorManager::iterate(
        std::function<void(const EventDescriptor&)> callback) {
    for (const auto& [k, v] : instance().descriptors) {
        callback(v);
    }
}
