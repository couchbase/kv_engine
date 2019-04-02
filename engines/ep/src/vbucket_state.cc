/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2019 Couchbase, Inc
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

#include "vbucket_state.h"
#include "vbucket.h"

bool vbucket_state::needsToBePersisted(const vbucket_state& vbstate) {
    /**
     * The vbucket state information is to be persisted
     * only if a change is detected in the state or the
     * failovers fields.
     */
    if (state != vbstate.state || failovers.compare(vbstate.failovers) != 0) {
        return true;
    }
    return false;
}

void vbucket_state::reset() {
    checkpointId = 0;
    maxDeletedSeqno = 0;
    highSeqno = 0;
    purgeSeqno = 0;
    lastSnapStart = 0;
    lastSnapEnd = 0;
    maxCas = 0;
    hlcCasEpochSeqno = HlcCasSeqnoUninitialised;
    mightContainXattrs = false;
    failovers.clear();
    supportsNamespaces = true;
}

void to_json(nlohmann::json& json, const vbucket_state& vbs) {
    // First add all required fields.
    // Note that integers are stored as strings to avoid any undesired
    // rounding (JSON in general only guarantees ~2^53 precision on integers).
    // While the current JSON library (nlohmann::json) _does_ support full
    // 64bit precision for integers, let's not rely on that for
    // all future uses.
    json = nlohmann::json{
            {"state", VBucket::toString(vbs.state)},
            {"checkpoint_id", std::to_string(vbs.checkpointId)},
            {"max_deleted_seqno", std::to_string(vbs.maxDeletedSeqno)},
            {"high_seqno", std::to_string(vbs.highSeqno)},
            {"purge_seqno", std::to_string(vbs.purgeSeqno)},
            {"snap_start", std::to_string(vbs.lastSnapStart)},
            {"snap_end", std::to_string(vbs.lastSnapEnd)},
            {"max_cas", std::to_string(vbs.maxCas)},
            {"hlc_epoch", std::to_string(vbs.hlcCasEpochSeqno)},
            {"might_contain_xattrs", vbs.mightContainXattrs},
            {"namespaces_supported", vbs.supportsNamespaces}};

    // Insert optional fields.
    if (!vbs.failovers.empty()) {
        json["failover_table"] = nlohmann::json::parse(vbs.failovers);
    }
}

void from_json(const nlohmann::json& j, vbucket_state& vbs) {
    // Parse required fields. Note that integers are stored as strings to avoid
    // any undesired rounding - see comment in to_json().
    vbs.state = VBucket::fromString(j.at("state").get<std::string>().c_str());
    vbs.checkpointId = std::stoull(j.at("checkpoint_id").get<std::string>());
    vbs.maxDeletedSeqno =
            std::stoull(j.at("max_deleted_seqno").get<std::string>());
    vbs.highSeqno = std::stoll(j.at("high_seqno").get<std::string>());
    vbs.purgeSeqno = std::stoull(j.at("purge_seqno").get<std::string>());
    vbs.lastSnapStart = std::stoull(j.at("snap_start").get<std::string>());
    vbs.lastSnapEnd = std::stoull(j.at("snap_end").get<std::string>());
    vbs.maxCas = std::stoull(j.at("max_cas").get<std::string>());
    vbs.hlcCasEpochSeqno = std::stoll(j.at("hlc_epoch").get<std::string>());
    vbs.mightContainXattrs = j.at("might_contain_xattrs").get<bool>();
    vbs.supportsNamespaces = j.at("namespaces_supported").get<bool>();

    // Now parse optional fields.
    auto failoverIt = j.find("failover_table");
    if (failoverIt != j.end()) {
        vbs.failovers = failoverIt->dump();
    }
}
