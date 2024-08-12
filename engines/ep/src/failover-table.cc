/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2014-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */

#include "failover-table.h"

#include "bucket_logger.h"

#include <nlohmann/json.hpp>
#include <platform/checked_snprintf.h>
#include <statistics/cbstat_collector.h>

FailoverTable::FailoverTable(size_t capacity)
    : max_entries(capacity), erroneousEntriesErased(0) {
    createEntry(0);
    cacheTableJSON();
}

FailoverTable::FailoverTable(const nlohmann::json& json,
                             size_t capacity,
                             int64_t highSeqno)
    : max_entries(capacity), erroneousEntriesErased(0) {
    if (!constructFromJSON(json)) {
        throw std::invalid_argument(
                fmt::format("FailoverTable(): unable to construct from JSON {}",
                            json.dump()));
    }
    sanitizeFailoverTable(highSeqno);
}

FailoverTable::~FailoverTable() = default;

failover_entry_t FailoverTable::getLatestEntry() const {
    std::lock_guard<std::mutex> lh(lock);
    return table.front();
}

void FailoverTable::removeLatestEntry() {
    std::lock_guard<std::mutex> lh(lock);
    if (!table.empty()) {
        table.pop_front();
        cacheTableJSON();
    }
}

uint64_t FailoverTable::getLatestUUID() const {
    return latest_uuid.load();
}

int64_t FailoverTable::generateUuid() {
    int64_t uuid;
    /* In past we have seen some erroneous entries in failover table with
       vb_uuid == 0 due to some bugs in the code which read/wrote the failover
       table from/to the disk or due to a some unknown buggy code.
       Hence we choose not to have 0 as a valid vb_uuid value. Loop below
       regenerates the vb_uuid in case 0 is generated by random generator */
    do {
        uuid = (provider.next() >> 16);
    } while (0 == uuid);
    return uuid;
}

void FailoverTable::createEntry(uint64_t high_seqno) {
    std::lock_guard<std::mutex> lh(lock);

    // Our failover table represents only *our* branch of history.
    // We must remove branches we've diverged from.
    // Entries that we remove here are not erroneous entries because a
    // diverged branch due to node(s) failure(s).
    table.remove_if([high_seqno](const failover_entry_t& e) {
                        return (e.by_seqno > high_seqno);
                    });

    failover_entry_t entry;
    entry.vb_uuid = generateUuid();
    entry.by_seqno = high_seqno;
    table.push_front(entry);
    latest_uuid = entry.vb_uuid;

    // Cap the size of the table
    while (table.size() > max_entries) {
        table.pop_back();
    }
    cacheTableJSON();
}

bool FailoverTable::getLastSeqnoForUUID(uint64_t uuid,
                                        uint64_t *seqno) const {
    std::lock_guard<std::mutex> lh(lock);
    auto curr_itr = table.begin();
    table_t::const_iterator prev_itr;

    if (curr_itr->vb_uuid == uuid) {
        return false;
    }

    prev_itr = curr_itr;

    ++curr_itr;

    for (; curr_itr != table.end(); ++curr_itr) {
        if (curr_itr->vb_uuid == uuid) {
            *seqno = prev_itr->by_seqno;
            return true;
        }

        prev_itr = curr_itr;
    }

    return false;
}

std::optional<FailoverTable::RollbackDetails> FailoverTable::needsRollback(
        uint64_t remoteHighSeqno,
        uint64_t localHighSeqno,
        uint64_t remoteVBUuid,
        uint64_t snapStartSeqno,
        uint64_t snapEndSeqno,
        uint64_t localPurgeSeqno,
        bool strictVbUuidMatch,
        std::optional<uint64_t> maxCollectionHighSeqno) const {
    /* Start with upper as vb highSeqno */
    uint64_t upper = localHighSeqno;
    std::lock_guard<std::mutex> lh(lock);

    /* Clients can have a diverging (w.r.t producer) branch at seqno 0 and in
       such a case, some of them strictly need a rollback and others don't.
       So we should NOT rollback when a client has a remoteVBUuid == 0 or
       if does not expect a rollback at remoteHighSeqno == 0 */
    if (remoteHighSeqno == 0 && (!strictVbUuidMatch || remoteVBUuid == 0)) {
        return {};
    }

    /*
     * If this request is for a collection stream then check if we can really
     * need to roll the client back if the remoteHighSeqno < localPurgeSeqno.
     * We should allow the request if the remoteHighSeqno indicates that the
     * client has all mutations/events for the collections the stream is for.
     */
    bool allowNonRollBackCollectionStream = false;
    if (maxCollectionHighSeqno.has_value()) {
        allowNonRollBackCollectionStream =
                remoteHighSeqno < localPurgeSeqno &&
                remoteHighSeqno >= maxCollectionHighSeqno.value() &&
                maxCollectionHighSeqno.value() <= localPurgeSeqno;
    }

    /* There may be items that are purged during compaction. We need
       to rollback to seq no 0 in that case, only if we have purged beyond
       remoteHighSeqno and if remoteHighSeqno is not 0 */
    if (remoteHighSeqno < localPurgeSeqno && remoteHighSeqno != 0 &&
        !allowNonRollBackCollectionStream) {
        return RollbackDetails{
                fmt::format("purge seqno ({}) is greater than start seqno - "
                            "could miss purged deletions",
                            localPurgeSeqno),
                0};
    }

    table_t::const_reverse_iterator itr;
    for (itr = table.rbegin(); itr != table.rend(); ++itr) {
        if (itr->vb_uuid == remoteVBUuid) {
            auto next = std::next(itr);
            if (next != table.rend()) {
                /* Since producer has more history we need to consider the
                   next seqno in failover table as upper */
                upper = next->by_seqno;
            }
            break;
        }
    }

    /* Find the rollback point */
    if (itr == table.rend()) {
        /* No remoteVBUuid match found in failover table, so producer and
         consumer have no common history. Rollback to zero */
        return RollbackDetails{
                "vBucket UUID not found in failover table, "
                "consumer and producer have no common history", 0};
    }

    // Optimization for avoiding unnecessary rollback when the client has
    // received a SnapshotMarker but no item for that snapshot.
    // Essentially here the client is only "virtually" in the snapshot it
    // declares, actually it is still at the end of the previous snapshot.
    // What is the "end of the previous snapshot"? The client's high-seqno by
    // logic.
    // By "moving backward" the snapEndSeqno we actually increase the
    // probability of hitting the following "no-rollback" condition.
    if (remoteHighSeqno == snapStartSeqno) {
        /* Client has no elements in the snapshot */
        snapEndSeqno = remoteHighSeqno;
    }

    if (snapEndSeqno <= upper) {
        /* No rollback needed as producer and consumer histories are same */
        return {};
    }

    // Optimization for avoiding unnecessary rollback when the client is in a
    // complete snapshot.
    if (remoteHighSeqno == snapEndSeqno) {
        /* Client already has all elements in the snapshot */
        snapStartSeqno = remoteHighSeqno;
    }

    /* We need a rollback as producer upper is lower than the end in
       consumer snapshot */
    uint64_t rollbackSeqno;
    if (upper < snapStartSeqno) {
        rollbackSeqno = upper;
    } else {
        /* We have to rollback till snapStartSeqno to handle
           deduplication case */
        rollbackSeqno = snapStartSeqno;
    }

    return RollbackDetails{fmt::format(
            "consumer ahead of producer - producer upper at {}",
            upper), rollbackSeqno};
}

void FailoverTable::pruneEntries(uint64_t seqno) {
    // Not permitted to remove the initial table entry (i.e. seqno zero).
    if (seqno == 0) {
        throw std::invalid_argument("FailoverTable::pruneEntries: "
                                    "cannot prune entry zero");
    }
    std::lock_guard<std::mutex> lh(lock);

    auto seqno_too_high = [seqno](failover_entry_t& entry) {
        return entry.by_seqno > seqno;
    };

    // Preconditions look good; remove them.
    table.remove_if(seqno_too_high);

    // If table is now empty, push a single element at high seqno. Throughout
    // FailoverTable we rely on there being at least one element.
    if (table.empty()) {
        failover_entry_t entry;
        entry.by_seqno = seqno;
        entry.vb_uuid = generateUuid();
        table.push_front(entry);
    }

    latest_uuid = table.front().vb_uuid;

    cacheTableJSON();
}

nlohmann::json FailoverTable::getJSON() {
    std::lock_guard<std::mutex> lh(lock);
    return cachedTableJSON;
}

void FailoverTable::cacheTableJSON() {
    nlohmann::json json = nlohmann::json::array();
    table_t::iterator it;
    for (it = table.begin(); it != table.end(); ++it) {
        nlohmann::json obj;
        obj["id"] = (*it).vb_uuid;
        obj["seq"] = (*it).by_seqno;
        json.push_back(obj);
    }
    cachedTableJSON = json;
}

void FailoverTable::addStats(CookieIface& cookie,
                             Vbid vbid,
                             const AddStatFn& add_stat) {
    std::lock_guard<std::mutex> lh(lock);
    try {
        std::array<char, 80> statname;
        checked_snprintf(statname.data(),
                         statname.size(),
                         "vb_%d:num_entries",
                         vbid.get());
        add_casted_stat(statname.data(), table.size(), add_stat, cookie);
        checked_snprintf(statname.data(),
                         statname.size(),
                         "vb_%d:num_erroneous_entries_erased",
                         vbid.get());
        add_casted_stat(statname.data(),
                        getNumErroneousEntriesErased(),
                        add_stat,
                        cookie);

        table_t::iterator it;
        int entrycounter = 0;
        for (it = table.begin(); it != table.end(); ++it) {
            checked_snprintf(statname.data(),
                             statname.size(),
                             "vb_%d:%d:id",
                             vbid.get(),
                             entrycounter);
            add_casted_stat(statname.data(), it->vb_uuid, add_stat, cookie);
            checked_snprintf(statname.data(),
                             statname.size(),
                             "vb_%d:%d:seq",
                             vbid.get(),
                             entrycounter);
            add_casted_stat(statname.data(), it->by_seqno, add_stat, cookie);
            entrycounter++;
        }
    } catch (std::exception& error) {
        EP_LOG_WARN("FailoverTable::addStats: Failed to build stats: {}",
                    error.what());
    }
}

std::vector<vbucket_failover_t> FailoverTable::getFailoverLog() {
    std::lock_guard<std::mutex> lh(lock);
    std::vector<vbucket_failover_t> result;
    for (const auto& entry : table) {
        vbucket_failover_t failoverEntry;
        failoverEntry.uuid = entry.vb_uuid;
        failoverEntry.seqno = entry.by_seqno;
        result.push_back(failoverEntry);
    }
    return result;
}

bool FailoverTable::constructFromJSON(const nlohmann::json& json) {
    if (!json.is_array()) {
        return false;
    }

    table_t new_table;

    for (const auto& it : json) {
        if (!it.is_object()) {
            return false;
        }

        auto jid = it.find("id");
        auto jseq = it.find("seq");

        if (jid == it.end() || !jid->is_number()) {
            return false;
        }
        if (jseq == it.end() || !jseq->is_number()) {
            return false;
        }

        failover_entry_t entry;
        entry.vb_uuid = *jid;
        entry.by_seqno = *jseq;
        new_table.push_back(entry);
    }

    // Must have at least one element in the failover table.
    if (new_table.empty()) {
        return false;
    }

    table = new_table;
    latest_uuid = table.front().vb_uuid;

    cachedTableJSON = json;
    return true;
}

void FailoverTable::replaceFailoverLog(const uint8_t* bytes, uint32_t length) {
    std::lock_guard<std::mutex> lh(lock);
    if ((length % 16) != 0 || length == 0) {
        throw std::invalid_argument("FailoverTable::replaceFailoverLog: "
                "length (which is " + std::to_string(length) +
                ") must be a non-zero multiple of 16");
    }
    table.clear();

    for (; length > 0; length -=16) {
        failover_entry_t entry;
        memcpy(&entry.by_seqno, bytes + length - 8, sizeof(uint64_t));
        memcpy(&entry.vb_uuid, bytes + length - 16, sizeof(uint64_t));
        entry.by_seqno = ntohll(entry.by_seqno);
        entry.vb_uuid = ntohll(entry.vb_uuid);
        table.push_front(entry);
    }

    latest_uuid = table.front().vb_uuid;

    cacheTableJSON();
}

size_t FailoverTable::getNumEntries() const
{
    return table.size();
}

void FailoverTable::sanitizeFailoverTable(int64_t highSeqno) {
    size_t intialTableSize = table.size();
    for (auto itr = table.begin(); itr != table.end(); ) {
        if (0 == itr->vb_uuid) {
            /* 1. Prune entries with vb_uuid == 0. (From past experience we have
                  seen erroneous entries mostly have vb_uuid == 0, hence we have
                  chosen not to use 0 as valid vb_uuid) */
            itr = table.erase(itr);
            continue;
        }
        if (itr != table.begin()) {
            auto prevItr = std::prev(itr);
            if (itr->by_seqno > prevItr->by_seqno) {
                /* 2. Prune any entry that has a by_seqno greater than by_seqno
                      of prev entry. (Entries are pushed at the head of the
                      table and must have seqno > seqno of following entries) */
                itr = table.erase(itr);
                continue;
            }
        }
        ++itr;
    }
    erroneousEntriesErased += (intialTableSize - table.size());

    if (table.empty()) {
        createEntry(highSeqno);
    } else if (erroneousEntriesErased) {
        cacheTableJSON();
    }
}

size_t FailoverTable::getNumErroneousEntriesErased() const {
    return erroneousEntriesErased;
}

std::ostream& operator<<(std::ostream& os, const failover_entry_t& entry) {
    os << R"({"vb_uuid":")" << entry.vb_uuid << R"(", "by_seqno":")"
       << entry.by_seqno << "\"}";
    return os;
}

std::ostream& operator<<(std::ostream& os, const FailoverTable& table) {
    std::lock_guard<std::mutex> lh(table.lock);
    os << "FailoverTable: max_entries:" << table.max_entries
       << ", erroneousEntriesErased:" << table.erroneousEntriesErased
       << ", latest_uuid:" << table.latest_uuid << "\n";
    os << "  cachedTableJSON:" << table.cachedTableJSON.dump() << "\n";
    os << "  table: {\n";
    for (const auto& e : table.table) {
        os << "    " << e << "\n";
    }
    os << "  }";

    return os;
}
