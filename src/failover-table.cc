/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2014 Couchbase, Inc
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
#include "config.h"

#include "atomic.h"
#include "failover-table.h"

FailoverTable::FailoverTable(size_t capacity) : max_entries(capacity), provider(true) { }

FailoverTable::FailoverTable() : max_entries(25), provider(true) { }

uint64_t FailoverTable::generateId() {
    return provider.next();
}

// Call when taking over as master to update failover table.
// id should be generated to be fairly likely to be unique.
void FailoverTable::createEntry(uint64_t id, uint64_t high_sequence) {
    failover_entry_t entry;
    entry.vb_uuid = id;
    entry.by_seqno = high_sequence;
    // Our failover table represents only *our* branch of history.
    // We must remove branches we've diverged from.
    pruneAbove(high_sequence);
    // and *then* add our entry
    table.push_front(entry);
    // Cap the size of the table
    while (table.size() > max_entries) {
        table.pop_back();
    }
}

// Where should client roll back to?
uint64_t FailoverTable::findRollbackPoint(uint64_t failover_id) {
    table_t::iterator it;
    for (it = table.begin(); it != table.end(); it++) {
        if ((*it).vb_uuid == failover_id) {
            if (it != table.begin()) {
                it--;
                return (*it).by_seqno;
            }
            // Shouldn't happen, as you should check that the failover id is not
            // the most recent
            return 0;
        }
    }
    return 0;
}

// Client should be rolled back?
bool FailoverTable::needsRollback(uint64_t since, uint64_t failover_id) {
    if (since == 0) {
        // Never need to roll back if rolling forward from 0
        return false;
    }

    if (failover_id == table.begin()->vb_uuid) {
        // Client is caught up w.r.t. failovers.
        return false;
    }

    uint64_t rollback_seq = findRollbackPoint(failover_id);
    if(since < rollback_seq) {
        // Client is behind the branch point, so a rollback would be
        // meaningless.
        return false;
    }

    return true;
}

// Prune entries above seq (Should call this any time we roll back!)
void FailoverTable::pruneAbove(uint64_t seq) {
    table_t::iterator it;
    for (it = table.begin(); it != table.end(); it++) {
        if ((*it).by_seqno >= seq) {
            it = table.erase(it);
        }
    }
}

std::string FailoverTable::toJSON() {
    cJSON* list = cJSON_CreateArray();
    table_t::iterator it;
    for(it = table.begin(); it != table.end(); it++) {
        cJSON* obj = cJSON_CreateObject();
        cJSON_AddNumberToObject(obj, "id", (*it).vb_uuid);
        cJSON_AddNumberToObject(obj, "seq", (*it).by_seqno);
        cJSON_AddItemToArray(list, obj);
    }
    char* json = cJSON_PrintUnformatted(list);
    std::string ret(json);
    free(json);
    cJSON_Delete(list);
    return ret;
}

bool FailoverTable::loadFromJSON(const std::string& json) {
    table.clear();

    cJSON* parsed = cJSON_Parse(json.c_str());

    bool ok = false;
    failover_entry_t e;

    if (parsed) {
        // Document must be an array
        ok = (parsed->type == cJSON_Array);
        if (!ok) {
            return ok;
        }

        for (cJSON* it = parsed->child; it != NULL; it = it->next) {
            // Inner elements must be objects
            ok = (it->type == cJSON_Object);
            if (!ok) {
                return ok;
            }

            // Transform row to entry
            ok = JSONtoEntry(it, e);
            if (!ok) {
                return ok;
            }

            // add to table
            table.push_back(e);
        }
    }
    return ok;
}

bool FailoverTable::JSONtoEntry(cJSON* jobj, failover_entry_t& entry) {
    cJSON* jid = cJSON_GetObjectItem(jobj, "id");
    cJSON* jseq = cJSON_GetObjectItem(jobj, "seq");
    if (!(jid && jseq)) return false;

    if (jid->type != cJSON_Number) return false;
    if (jseq->type != cJSON_Number) return false;

    entry.vb_uuid = (uint64_t) jid->valuedouble;
    entry.by_seqno = (uint64_t) jseq->valuedouble;
    return true;
}

FailoverTable::~FailoverTable() { }
