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
#define STATWRITER_NAMESPACE failovers
#include "statwriter.h"
#undef STATWRITER_NAMESPACE

FailoverTable::FailoverTable(size_t capacity)
    : max_entries(capacity), provider(true) {
    createEntry(0);
}

FailoverTable::FailoverTable(const std::string& json, size_t capacity)
    : max_entries(capacity), provider(true) {
    loadFromJSON(json);
    assert(table.size() > 0);
}

FailoverTable::~FailoverTable() { }

failover_entry_t FailoverTable::getLatestEntry() {
    LockHolder lh(lock);
    return table.front();
}

void FailoverTable::createEntry(uint64_t high_seqno) {
    LockHolder lh(lock);
    // Our failover table represents only *our* branch of history.
    // We must remove branches we've diverged from.
    table_t::iterator itr = table.begin();
    for (; itr != table.end(); ++itr) {
        if (itr->by_seqno > high_seqno) {
            itr = table.erase(itr);
        }
    }

    if (table.empty() || table.front().by_seqno != high_seqno) {
        failover_entry_t entry;
        entry.vb_uuid = provider.next();
        entry.by_seqno = high_seqno;
        table.push_front(entry);
    }

    // Cap the size of the table
    while (table.size() > max_entries) {
        table.pop_back();
    }
}

bool FailoverTable::needsRollback(uint64_t start_seqno, uint64_t cur_seqno,
                                  uint64_t vb_uuid, uint64_t high_seqno,
                                  uint64_t* rollback_seqno) {
    LockHolder lh(lock);
    *rollback_seqno = 0;
    if (start_seqno == 0) {
        return false;
    }

    table_t::reverse_iterator itr;
    for (itr = table.rbegin(); itr != table.rend(); ++itr) {
        if (itr->vb_uuid == vb_uuid && itr->by_seqno == high_seqno) {
            uint64_t lower = itr->by_seqno;
            uint64_t upper = cur_seqno;

            ++itr;
            if (itr != table.rend()) {
                upper = itr->by_seqno;
            }

            if (start_seqno >= lower && start_seqno <= upper) {
                return false;
            }
            *rollback_seqno = upper;
            return true;
        }
    }

    return true;
}

std::string FailoverTable::toJSON() {
    LockHolder lh(lock);
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

void FailoverTable::addStats(const void* cookie, uint16_t vbid,
                             ADD_STAT add_stat) {
    LockHolder lh(lock);
    char statname[80] = {0};
    snprintf(statname, 80, "failovers:vb_%d:num_entries", vbid);
    add_casted_stat(statname, table.size(), add_stat, cookie);

    table_t::iterator it;
    int entrycounter = 0;
    for(it = table.begin(); it != table.end(); ++it) {
        snprintf(statname, 80, "failovers:vb_%d:%d:id", vbid, entrycounter);
        add_casted_stat(statname, it->vb_uuid, add_stat, cookie);
        snprintf(statname, 80, "failovers:vb_%d:%d:seq", vbid, entrycounter);
        add_casted_stat(statname, it->by_seqno, add_stat, cookie);
        entrycounter++;
    }
}

ENGINE_ERROR_CODE FailoverTable::addFailoverLog(const void* cookie,
                                                upr_add_failover_log callback) {
    LockHolder lh(lock);
    ENGINE_ERROR_CODE rv = ENGINE_SUCCESS;
    size_t logsize = table.size();

    assert(logsize > 0);
    vbucket_failover_t *logentries = new vbucket_failover_t[logsize];
    vbucket_failover_t *logentry = logentries;

    table_t::iterator itr;
    for(itr = table.begin(); itr != table.end(); ++itr) {
        logentry->uuid = itr->vb_uuid;
        logentry->seqno = itr->by_seqno;
        logentry++;
    }
    rv = callback(logentries, logsize, cookie);
    delete[] logentries;

    return rv;
}

bool FailoverTable::loadFromJSON(const std::string& json) {
    cJSON* parsed = cJSON_Parse(json.c_str());

    if (parsed) {
        if (parsed->type != cJSON_Array) {
            return false;
        }

        for (cJSON* it = parsed->child; it != NULL; it = it->next) {
            if (it->type != cJSON_Object) {
                return false;
            }

            cJSON* jid = cJSON_GetObjectItem(it, "id");
            cJSON* jseq = cJSON_GetObjectItem(it, "seq");

            if (jid && jid->type != cJSON_Number) {
                return false;
            }
            if (jseq && jseq->type != cJSON_Number){
                return false;
            }

            failover_entry_t entry;
            entry.vb_uuid = (uint64_t) jid->valuedouble;
            entry.by_seqno = (uint64_t) jseq->valuedouble;
            table.push_back(entry);
        }
    }
    return true;
}

void FailoverTable::replaceFailoverLog(uint8_t* bytes, uint32_t length) {
    LockHolder lh(lock);
    assert((length % 16) == 0 && length != 0);
    table.clear();

    for (; length > 0; length -=16) {
        failover_entry_t entry;
        memcpy(&entry.by_seqno, bytes + length - 8, sizeof(uint64_t));
        memcpy(&entry.vb_uuid, bytes + length - 16, sizeof(uint64_t));
        entry.by_seqno = ntohll(entry.by_seqno);
        entry.vb_uuid = ntohll(entry.vb_uuid);
        table.push_front(entry);
    }
}
