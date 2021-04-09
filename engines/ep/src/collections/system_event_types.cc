/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2021 Couchbase, Inc
 *
 *   Licensed under the Apache License, Version 2.0 (the "License");
 *   you may not use this file except in compliance with the License.
 *   You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 *   Unless required by applicable law or agreed to in writing, software
 *   distributed under the License is distributed on an "AS IS" BASIS,
 *   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *   See the License for the specific language governing permissions and
 *   limitations under the License.
 */

#include "collections/system_event_types.h"

#include <spdlog/fmt/fmt.h>

namespace Collections {

std::string to_string(const CreateEventData& event) {
    return fmt::format(
            fmt("CreateCollection{{uid:{:#x} scopeID:{} collectionID:{} "
                "name:'"
                "{}' maxTTLEnabled:{} maxTTL:{}}}"),
            event.manifestUid.load(),
            event.metaData.sid.to_string(),
            event.metaData.cid.to_string(),
            event.metaData.name,
            event.metaData.maxTtl.has_value(),
            event.metaData.maxTtl.has_value() ? event.metaData.maxTtl->count()
                                              : 0);
}

std::string to_string(const DropEventData& event) {
    return fmt::format(
            fmt("DropCollection{{uid:{:#x} scopeID:{} collectionID:{}}}"),
            event.manifestUid.load(),
            event.sid.to_string(),
            event.cid.to_string());
}

std::string to_string(const CreateScopeEventData& event) {
    return fmt::format(fmt("CreateScope{{uid:{:#x} scopeID:{} name:'{}'}}"),
                       event.manifestUid.load(),
                       event.metaData.sid.to_string(),
                       event.metaData.name);
}

std::string to_string(const DropScopeEventData& event) {
    return fmt::format(fmt("DropScope{{uid:{:#x} scopeID:{}}}"),
                       event.manifestUid.load(),
                       event.sid.to_string());
}

} // namespace Collections