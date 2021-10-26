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

#include "collections/system_event_types.h"

#include <spdlog/fmt/fmt.h>

namespace Collections {

std::string to_string(const CreateEventData& event) {
    return fmt::format(FMT_STRING("CreateCollection{{uid:{:#x} scopeID:{} "
                                  "collectionID:{} name:'{}' "
                                  "maxTTLEnabled:{} maxTTL:{}}}"),
                       event.manifestUid.load(),
                       event.metaData.sid.to_string(),
                       event.metaData.cid.to_string(),
                       event.metaData.name,
                       event.metaData.maxTtl.has_value(),
                       event.metaData.maxTtl.has_value()
                               ? event.metaData.maxTtl->count()
                               : 0);
}

std::string to_string(const DropEventData& event) {
    return fmt::format(
            FMT_STRING(
                    "DropCollection{{uid:{:#x} scopeID:{} collectionID:{}}}"),
            event.manifestUid.load(),
            event.sid.to_string(),
            event.cid.to_string());
}

std::string to_string(const CreateScopeEventData& event) {
    return fmt::format(
            FMT_STRING("CreateScope{{uid:{:#x} scopeID:{} name:'{}'}}"),
            event.manifestUid.load(),
            event.metaData.sid.to_string(),
            event.metaData.name);
}

std::string to_string(const DropScopeEventData& event) {
    return fmt::format(FMT_STRING("DropScope{{uid:{:#x} scopeID:{}}}"),
                       event.manifestUid.load(),
                       event.sid.to_string());
}

bool operator==(const CreateEventDcpData& lhs, const CreateEventDcpData& rhs) {
    return lhs.manifestUid.uid == rhs.manifestUid.uid &&
           lhs.sid.to_host() == rhs.sid.to_host() &&
           lhs.cid.to_host() == rhs.cid.to_host();
}

bool operator==(const CreateWithMaxTtlEventDcpData& lhs,
                const CreateWithMaxTtlEventDcpData& rhs) {
    return lhs.manifestUid.uid == rhs.manifestUid.uid &&
           lhs.sid.to_host() == rhs.sid.to_host() &&
           lhs.cid.to_host() == rhs.cid.to_host() && lhs.maxTtl == rhs.maxTtl;
}

bool operator==(const DropEventDcpData& lhs, const DropEventDcpData& rhs) {
    return lhs.manifestUid.uid == rhs.manifestUid.uid &&
           lhs.cid.to_host() == rhs.cid.to_host() &&
           lhs.sid.to_host() == rhs.sid.to_host();
}

bool operator==(const CreateScopeEventDcpData& lhs,
                const CreateScopeEventDcpData& rhs) {
    return lhs.manifestUid.uid == rhs.manifestUid.uid &&
           lhs.sid.to_host() == rhs.sid.to_host();
}

bool operator==(const DropScopeEventDcpData& lhs,
                const DropScopeEventDcpData& rhs) {
    return lhs.manifestUid.uid == rhs.manifestUid.uid &&
           lhs.sid.to_host() == rhs.sid.to_host();
}

bool operator!=(const CreateEventDcpData& lhs, const CreateEventDcpData& rhs) {
    return !(lhs == rhs);
}

bool operator!=(const CreateWithMaxTtlEventDcpData& lhs,
                const CreateWithMaxTtlEventDcpData& rhs) {
    return !(lhs == rhs);
}

bool operator!=(const DropEventDcpData& lhs, const DropEventDcpData& rhs) {
    return !(lhs == rhs);
}

bool operator!=(const CreateScopeEventDcpData& lhs,
                const CreateScopeEventDcpData& rhs) {
    return !(lhs == rhs);
}

} // namespace Collections
