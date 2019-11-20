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
#include "item.h"
#include "vbucket.h"

bool vbucket_transition_state::needsToBePersisted(
        const vbucket_transition_state& transition) {
    return state != transition.state || failovers != transition.failovers ||
           replicationTopology != transition.replicationTopology;
}

void vbucket_transition_state::toItem(Item& item) const {
    nlohmann::json json = *this;
    std::string jsonState = json.dump();
    item.replaceValue(Blob::New(jsonState.data(), jsonState.size()));
}

void vbucket_transition_state::fromItem(const Item& item) {
    std::string jsonState(item.getData(), item.getNBytes());
    nlohmann::json json;
    try {
        json = nlohmann::json::parse(jsonState);
    } catch (const nlohmann::json::exception& e) {
        throw std::logic_error(
                "vbucket_transition_state::fromItem cannot decode json " +
                jsonState + " " + e.what());
    }
    try {
        *this = json;
    } catch (const nlohmann::json::exception& e) {
        throw std::logic_error(
                "vbucket_transition_state::fromItem cannot convert json " +
                jsonState + " " + e.what());
    }
}

bool vbucket_transition_state::operator==(
        const vbucket_transition_state& other) const {
    bool rv = true;
    rv = rv && (failovers == other.failovers);
    rv = rv && (replicationTopology == other.replicationTopology);
    rv = rv && (state == other.state);
    return rv;
}

bool vbucket_transition_state::operator!=(
        const vbucket_transition_state& other) const {
    return !(*this == other);
}

bool vbucket_state::needsToBePersisted(const vbucket_state& vbstate) {
    /*
     * The vbucket state information is to be persisted only if a change is
     * detected in:
     * - the state
     * - the failover table, or
     * - the replication topology or
     *   (above owned by struct vbucket_transition_state)
     * - the persisted completed seqno or
     * - the persisted prepared seqno or
     * - the high prepared seqno
     */
    return (transition.needsToBePersisted(vbstate.transition) ||
            persistedCompletedSeqno != vbstate.persistedCompletedSeqno ||
            persistedPreparedSeqno != vbstate.persistedPreparedSeqno ||
            highPreparedSeqno != vbstate.highPreparedSeqno ||
            maxVisibleSeqno != vbstate.maxVisibleSeqno);
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
    supportsNamespaces = true;
    version = CurrentVersion;
    persistedCompletedSeqno = 0;
    persistedPreparedSeqno = 0;
    highPreparedSeqno = 0;
    maxVisibleSeqno = 0;
    onDiskPrepares = 0;
    transition = vbucket_transition_state{};
}

bool vbucket_state::operator==(const vbucket_state& other) const {
    bool rv = true;
    rv = rv && (maxDeletedSeqno == other.maxDeletedSeqno);
    rv = rv && (highSeqno == other.highSeqno);
    rv = rv && (purgeSeqno == other.purgeSeqno);
    rv = rv && (lastSnapStart == other.lastSnapStart);
    rv = rv && (lastSnapEnd == other.lastSnapEnd);
    rv = rv && (maxCas == other.maxCas);
    rv = rv && (hlcCasEpochSeqno == other.hlcCasEpochSeqno);
    rv = rv && (mightContainXattrs == other.mightContainXattrs);
    rv = rv && (supportsNamespaces == other.supportsNamespaces);
    rv = rv && (version == other.version);
    rv = rv && (persistedCompletedSeqno == other.persistedCompletedSeqno);
    rv = rv && (persistedPreparedSeqno == other.persistedPreparedSeqno);
    rv = rv && (highPreparedSeqno ==
                other.highPreparedSeqno);
    rv = rv && (maxVisibleSeqno == other.maxVisibleSeqno);
    rv = rv && (onDiskPrepares == other.onDiskPrepares);
    rv = rv && (checkpointType == other.checkpointType);
    rv = rv && (transition == other.transition);
    return rv;
}

bool vbucket_state::operator!=(const vbucket_state& other) const {
    return !(*this == other);
}

void to_json(nlohmann::json& json, const vbucket_state& vbs) {
    // First add all required fields.
    // Note that integers are stored as strings to avoid any undesired
    // rounding (JSON in general only guarantees ~2^53 precision on integers).
    // While the current JSON library (nlohmann::json) _does_ support full
    // 64bit precision for integers, let's not rely on that for
    // all future uses.
    json = nlohmann::json{
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
            {"completed_seqno", std::to_string(vbs.persistedCompletedSeqno)},
            {"prepared_seqno", std::to_string(vbs.persistedPreparedSeqno)},
            {"high_prepared_seqno", std::to_string(vbs.highPreparedSeqno)},
            {"max_visible_seqno", std::to_string(vbs.maxVisibleSeqno)},
            {"on_disk_prepares", std::to_string(vbs.onDiskPrepares)},
            {"checkpoint_type", to_string(vbs.checkpointType)}};

    to_json(json, vbs.transition);
}

void from_json(const nlohmann::json& j, vbucket_state& vbs) {
    // Parse required fields. Note that integers are stored as strings to avoid
    // any undesired rounding - see comment in to_json().
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
    auto version = j.find("version");
    if (version != j.end()) {
        vbs.version = (*version).get<int>();
    } else {
        vbs.version = 1;
    }

    // Note: We don't have any HCS in pre-6.5
    auto hcs = j.find("completed_seqno");
    if (hcs != j.end()) {
        vbs.persistedCompletedSeqno = std::stoull((*hcs).get<std::string>());
    } else {
        vbs.persistedCompletedSeqno = 0;
    }

    // Note: We don't have any PPS in pre-6.5
    auto pps = j.find("prepared_seqno");
    if (pps != j.end()) {
        vbs.persistedPreparedSeqno = std::stoull((*pps).get<std::string>());
    } else {
        vbs.persistedPreparedSeqno = 0;
    }

    // Note: We don't have any HPS in pre-6.5
    auto hps = j.find("high_prepared_seqno");
    if (hps != j.end()) {
        vbs.highPreparedSeqno = std::stoull((*hps).get<std::string>());
    } else {
        vbs.highPreparedSeqno = 0;
    }

    // Note: We don't have a maxVisibleSeqno in pre-6.5
    auto maxVisibleSeqno = j.find("max_visible_seqno");
    if (maxVisibleSeqno != j.end()) {
        vbs.maxVisibleSeqno =
                std::stoull((*maxVisibleSeqno).get<std::string>());
    } else {
        vbs.maxVisibleSeqno = 0;
    }

    // Note: We don't track on disk prepares pre-6.5
    vbs.onDiskPrepares = std::stoll(j.value("on_disk_prepares", "0"));

    // Note: We don't track checkpoint type pre-6.5
    auto checkpointType = j.find("checkpoint_type");
    if (checkpointType != j.end()) {
        auto str = checkpointType->get<std::string>();
        if (str == "Disk") {
            vbs.checkpointType = CheckpointType::Disk;
        } else if (str == "Memory") {
            vbs.checkpointType = CheckpointType::Memory;
        } else {
            throw std::invalid_argument(
                    "VBucketState::from_json checkpointType was not an "
                    "expected value: " +
                    str);
        }
    }

    from_json(j, vbs.transition);
}

void to_json(nlohmann::json& json, const vbucket_transition_state& state) {
    json["state"] = VBucket::toString(state.state);

    // Insert optional fields.
    if (!state.failovers.empty()) {
        json["failover_table"] = nlohmann::json::parse(state.failovers);
    }
    if (!state.replicationTopology.empty()) {
        json["replication_topology"] = state.replicationTopology;
    }
}

void from_json(const nlohmann::json& j, vbucket_transition_state& state) {
    state.state = VBucket::fromString(j.at("state").get<std::string>().c_str());

    // Now check for optional fields.
    auto failoverIt = j.find("failover_table");
    if (failoverIt != j.end()) {
        state.failovers = failoverIt->dump();
    }

    auto topologyIt = j.find("replication_topology");
    if (topologyIt != j.end()) {
        state.replicationTopology = *topologyIt;
    }
}

std::ostream& operator<<(std::ostream& os, const vbucket_state& vbs) {
    nlohmann::json j;
    to_json(j, vbs);
    os << j;
    return os;
}

std::ostream& operator<<(std::ostream& os,
                         const vbucket_transition_state& state) {
    nlohmann::json j;
    to_json(j, state);
    os << j;
    return os;
}
