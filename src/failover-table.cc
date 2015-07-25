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
#include "locks.h"
#define STATWRITER_NAMESPACE failovers
#include "statwriter.h"
#undef STATWRITER_NAMESPACE

FailoverTable::FailoverTable(size_t capacity)
    : max_entries(capacity), provider(true) {
    createEntry(0);
    cacheTableJSON();
}

FailoverTable::FailoverTable(const std::string& json, size_t capacity)
    : max_entries(capacity), provider(true) {
    loadFromJSON(json);
    cb_assert(table.size() > 0);
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

    failover_entry_t entry;
    entry.vb_uuid = (provider.next() >> 16);
    entry.by_seqno = high_seqno;
    table.push_front(entry);

    // Cap the size of the table
    while (table.size() > max_entries) {
        table.pop_back();
    }
    cacheTableJSON();
}

bool FailoverTable::needsRollback(uint64_t start_seqno,
                                  uint64_t cur_seqno,
                                  uint64_t vb_uuid,
                                  uint64_t snap_start_seqno,
                                  uint64_t snap_end_seqno,
                                  uint64_t purge_seqno,
                                  uint64_t* rollback_seqno) {
    LockHolder lh(lock);
    if (start_seqno == 0) {
        return false;
    }

    *rollback_seqno = 0;

    /* To be more efficient (avoid unnecessary rollback), see if the client has
       already received all items in the requested snapshot */
    if (start_seqno == snap_end_seqno) {
        snap_start_seqno = start_seqno;
    }

    /* There may be items that are purged during compaction. We need
     to rollback to seq no 0 in that case */
    if (snap_start_seqno < purge_seqno) {
        return true;
    }

    table_t::reverse_iterator itr;
    for (itr = table.rbegin(); itr != table.rend(); ++itr) {
        if (itr->vb_uuid == vb_uuid) {
            uint64_t upper = cur_seqno;

            ++itr;
            if (itr != table.rend()) {
                upper = itr->by_seqno;
            }

            if (snap_end_seqno <= upper) {
                return false;
            }

            if (snap_start_seqno == start_seqno && upper == start_seqno) {
                return false;
            }

            if (snap_start_seqno >= upper) {
                *rollback_seqno = upper;
                return true;
            }

            if (snap_start_seqno == start_seqno) {
                return false;
            }

            *rollback_seqno = snap_start_seqno;
            return true;
        }
    }

    return true;
}

void FailoverTable::pruneEntries(uint64_t seqno) {
    LockHolder lh(lock);
    table_t::iterator it = table.begin();
    for (; it != table.end(); ++it) {
        if (it->by_seqno > seqno) {
            it = table.erase(it);
        }
    }

    cacheTableJSON();
}

std::string FailoverTable::toJSON() {
    LockHolder lh(lock);
    return cachedTableJSON;
}

void FailoverTable::cacheTableJSON() {
    cJSON* list = cJSON_CreateArray();
    table_t::iterator it;
    for(it = table.begin(); it != table.end(); it++) {
        cJSON* obj = cJSON_CreateObject();
        cJSON_AddNumberToObject(obj, "id", (*it).vb_uuid);
        cJSON_AddNumberToObject(obj, "seq", (*it).by_seqno);
        cJSON_AddItemToArray(list, obj);
    }
    char* json = cJSON_PrintUnformatted(list);
    cachedTableJSON = json;
    free(json);
    cJSON_Delete(list);
}

void FailoverTable::addStats(const void* cookie, uint16_t vbid,
                             ADD_STAT add_stat) {
    LockHolder lh(lock);
    char statname[80] = {0};
    snprintf(statname, 80, "vb_%d:num_entries", vbid);
    add_casted_stat(statname, table.size(), add_stat, cookie);

    table_t::iterator it;
    int entrycounter = 0;
    for(it = table.begin(); it != table.end(); ++it) {
        snprintf(statname, 80, "vb_%d:%d:id", vbid, entrycounter);
        add_casted_stat(statname, it->vb_uuid, add_stat, cookie);
        snprintf(statname, 80, "vb_%d:%d:seq", vbid, entrycounter);
        add_casted_stat(statname, it->by_seqno, add_stat, cookie);
        entrycounter++;
    }
}

ENGINE_ERROR_CODE FailoverTable::addFailoverLog(const void* cookie,
                                                dcp_add_failover_log callback) {
    LockHolder lh(lock);
    ENGINE_ERROR_CODE rv = ENGINE_SUCCESS;
    size_t logsize = table.size();

    cb_assert(logsize > 0);
    vbucket_failover_t *logentries = new vbucket_failover_t[logsize];
    vbucket_failover_t *logentry = logentries;

    table_t::iterator itr;
    for(itr = table.begin(); itr != table.end(); ++itr) {
        logentry->uuid = itr->vb_uuid;
        logentry->seqno = itr->by_seqno;
        logentry++;
    }
    EventuallyPersistentEngine *epe = ObjectRegistry::onSwitchThread(NULL, true);
    rv = callback(logentries, logsize, cookie);
    ObjectRegistry::onSwitchThread(epe);
    delete[] logentries;

    return rv;
}

bool FailoverTable::loadFromJSON(cJSON *json) {
    if (json->type != cJSON_Array) {
        return false;
    }

    for (cJSON* it = json->child; it != NULL; it = it->next) {
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

    return true;
}

bool FailoverTable::loadFromJSON(const std::string& json) {
    cJSON* parsed = cJSON_Parse(json.c_str());
    bool ret = true;

    if (parsed) {
        ret = loadFromJSON(parsed);
        cachedTableJSON = json;
        cJSON_Delete(parsed);
    }

    return ret;
}

void FailoverTable::replaceFailoverLog(uint8_t* bytes, uint32_t length) {
    LockHolder lh(lock);
    cb_assert((length % 16) == 0 && length != 0);
    table.clear();

    for (; length > 0; length -=16) {
        failover_entry_t entry;
        memcpy(&entry.by_seqno, bytes + length - 8, sizeof(uint64_t));
        memcpy(&entry.vb_uuid, bytes + length - 16, sizeof(uint64_t));
        entry.by_seqno = ntohll(entry.by_seqno);
        entry.vb_uuid = ntohll(entry.vb_uuid);
        table.push_front(entry);
    }
    cacheTableJSON();
}
