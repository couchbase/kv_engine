/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2021-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */

#include "collections/vbucket_manifest_scope_entry.h"

#include <ostream>

bool Collections::VB::ScopeEntry::operator==(const ScopeEntry& other) const {
    return getName() == other.getName() &&
           getDataSize() == other.getDataSize() &&
           getDataLimit() == other.getDataLimit();
}

std::ostream& Collections::VB::operator<<(
        std::ostream& os, const Collections::VB::ScopeEntry& scopeEntry) {
    os << "ScopeEntry:"
       << " name:" << scopeEntry.getName()
       << ", dataSize:" << scopeEntry.getDataSize();
    if (scopeEntry.getDataLimit()) {
        os << ", dataLimit:" << scopeEntry.getDataLimit().value();
    }
    return os;
}