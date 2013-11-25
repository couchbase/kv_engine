#ifndef SRC_FAILOVER_TABLE_H_
#define SRC_FAILOVER_TABLE_H_ 1

#include <deque>
#include <list>
#include <string>
#include <stdint.h>
#include <stddef.h>
#include <stdlib.h>

#include "cJSON.h"


class FailoverTable {
  public:
    typedef std::pair<uint64_t, uint64_t> entry_t;
    typedef std::list<entry_t> table_t;

    FailoverTable(size_t capacity) : max_entries(capacity) {
    }

    FailoverTable() : max_entries(25) {
    }

    FailoverTable(const FailoverTable& other) {
      max_entries = other.max_entries;
      table = other.table;
    }

    // This should probably be replaced with something better.
    uint64_t generateId() {
      return (uint64_t) gethrtime();
    }

    // Call when taking over as master to update failover table.
    // id should be generated to be fairly likely to be unique.
    void createEntry(uint64_t id, uint64_t high_sequence) {
      entry_t entry;
      entry.first = id;
      entry.second = high_sequence;
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
    uint64_t findRollbackPoint(uint64_t failover_id) {
      table_t::iterator it;
      for(it = table.begin(); it != table.end(); it++) {
        if((*it).first == failover_id) {
          if(it != table.begin()) {
            it--;
            return (*it).second;
          }
          // Shouldn't happen, as you should check that the failover id is not
          // the most recent
          return 0;
        }
      }
      return 0;
    }

    // Client should be rolled back?
    bool needsRollback(uint64_t since, uint64_t failover_id) {
      if(since == 0) {
        // Never need to roll back if rolling forward from 0
        return false;
      }

      if(failover_id == table.begin()->first) {
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
    void pruneAbove(uint64_t seq) {
      table_t::iterator it;
      for(it = table.begin(); it != table.end(); it++) {
        if((*it).second >= seq) {
          it = table.erase(it);
        }
      }
    }

    std::string toJSON() {
      cJSON* list = cJSON_CreateArray();
      table_t::iterator it;
      for(it = table.begin(); it != table.end(); it++) {
        cJSON* obj = cJSON_CreateObject();
        cJSON_AddNumberToObject(obj, "id", (*it).first);
        cJSON_AddNumberToObject(obj, "seq", (*it).second);
        cJSON_AddItemToArray(list, obj);
      }
      char* json = cJSON_PrintUnformatted(list);
      std::string ret(json);
      free(json);
      cJSON_Delete(list);
      return ret;
    }

    bool loadFromJSON(cJSON* parsed) {
      table.clear();

      bool ok = false;
      entry_t e;

      if(parsed) {
        // Document must be an array
        ok = (parsed->type == cJSON_Array);
        if(!ok) goto error;

        for (cJSON* it = parsed->child; it != NULL; it = it->next) {
          // Inner elements must be objects
          ok = (it->type == cJSON_Object);
          if(!ok) goto error;

          // Transform row to entry
          ok = JSONtoEntry(it, e);
          if(!ok) goto error;

          // add to table
          table.push_back(e);
        }
      }
error:
      return ok;
    }

    table_t table;
    size_t max_entries;

  private:
    bool JSONtoEntry(cJSON* jobj, entry_t& entry) {
      cJSON* jid = cJSON_GetObjectItem(jobj, "id");
      cJSON* jseq = cJSON_GetObjectItem(jobj, "seq");
      if(!(jid && jseq)) return false;

      if(jid->type != cJSON_Number) return false;
      if(jseq->type != cJSON_Number) return false;

      entry.first = (uint64_t) jid->valuedouble;
      entry.second = (uint64_t) jseq->valuedouble;
      return true;
    }
};

#endif

