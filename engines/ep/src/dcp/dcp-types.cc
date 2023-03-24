/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2023-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */

#include "dcp-types.h"
#include <string>

std::string dcpMarkerFlagsToString(uint32_t dcpMarkerFlags) {
    std::string s = "[";
    if (dcpMarkerFlags & MARKER_FLAG_MEMORY) {
        s.append("MEMORY,");
    }
    if (dcpMarkerFlags & MARKER_FLAG_DISK) {
        s.append("DISK,");
    }
    if (dcpMarkerFlags & MARKER_FLAG_CHK) {
        s.append("CHK,");
    }
    if (dcpMarkerFlags & MARKER_FLAG_ACK) {
        s.append("ACK,");
    }
    if (dcpMarkerFlags & MARKER_FLAG_HISTORY) {
        s.append("HISTORY,");
    }
    if (dcpMarkerFlags & MARKER_FLAG_MAY_CONTAIN_DUPLICATE_KEYS) {
        s.append("MAY_CONTAIN_DUPLICATE_KEYS,");
    }
    if (s.size() > 1) {
        s.pop_back();
    }
    s.append("]");
    return s;
}