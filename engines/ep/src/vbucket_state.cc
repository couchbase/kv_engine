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
    /*
     * The vbucket state information is to be persisted only if a change is
     * detected in:
     * - the state
     * - the failover table, or
     * - the replication topology or
     * - the high completed seqno or
     * - the high prepared seqno
     */
    return (state != vbstate.state || failovers != vbstate.failovers ||
            replicationTopology != vbstate.replicationTopology ||
            highCompletedSeqno != vbstate.highCompletedSeqno ||
            highPreparedSeqno != vbstate.highPreparedSeqno);
}

void vbucket_state::reset() {
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
    replicationTopology.clear();
    version = CurrentVersion;
    highCompletedSeqno = 0;
    highPreparedSeqno = 0;
    onDiskPrepares = 0;
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
            {"max_deleted_seqno", std::to_string(vbs.maxDeletedSeqno)},
            {"high_seqno", std::to_string(vbs.highSeqno)},
            {"purge_seqno", std::to_string(vbs.purgeSeqno)},
            {"snap_start", std::to_string(vbs.lastSnapStart)},
            {"snap_end", std::to_string(vbs.lastSnapEnd)},
            {"max_cas", std::to_string(vbs.maxCas)},
            {"hlc_epoch", std::to_string(vbs.hlcCasEpochSeqno)},
            {"might_contain_xattrs", vbs.mightContainXattrs},
            {"namespaces_supported", vbs.supportsNamespaces},
            {"version", vbs.version},
            {"high_completed_seqno", std::to_string(vbs.highCompletedSeqno)},
            {"high_prepared_seqno", std::to_string(vbs.highPreparedSeqno)},
            {"on_disk_prepares", std::to_string(vbs.onDiskPrepares)}};

    // Insert optional fields.
    if (!vbs.failovers.empty()) {
        json["failover_table"] = nlohmann::json::parse(vbs.failovers);
    }
    if (!vbs.replicationTopology.empty()) {
        json["replication_topology"] = vbs.replicationTopology;
    }
}

void from_json(const nlohmann::json& j, vbucket_state& vbs) {
    // Parse required fields. Note that integers are stored as strings to avoid
    // any undesired rounding - see comment in to_json().
    vbs.state = VBucket::fromString(j.at("state").get<std::string>().c_str());
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

    auto topologyIt = j.find("replication_topology");
    if (topologyIt != j.end()) {
        vbs.replicationTopology = *topologyIt;
    }

    auto version = j.find("version");
    if (version != j.end()) {
        vbs.version = (*version).get<int>();
    } else {
        vbs.version = 1;
    }

    // Note: We don't have any HCS in pre-6.5
    auto hcs = j.find("high_completed_seqno");
    if (hcs != j.end()) {
        vbs.highCompletedSeqno = std::stoull((*hcs).get<std::string>());
    } else {
        vbs.highCompletedSeqno = 0;
    }

    // Note: We don't have any HPS in pre-6.5
    auto hps = j.find("high_prepared_seqno");
    if (hps != j.end()) {
        vbs.highPreparedSeqno = std::stoull((*hps).get<std::string>());
    } else {
        vbs.highPreparedSeqno = 0;
    }

    // Note: We don't track on disk prepares pre-6.5
    vbs.onDiskPrepares = std::stoll(j.value("on_disk_prepares", "0"));
}

std::ostream& operator<<(std::ostream& os, const vbucket_state& vbs) {
    nlohmann::json j;
    to_json(j, vbs);
    os << j;
    return os;
}
