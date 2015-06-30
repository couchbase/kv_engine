/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2015 Couchbase, Inc
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
#pragma once

#include <platform/cbassert.h>
#include <memcached/engine.h>
#include <cJSON.h>
#include <mutex>
#include <list>
#include <string>
#include <map>

typedef struct dlist {
    struct dlist *next;
    struct dlist *prev;
} dlist_t;

typedef struct {
    dlist_t ti_list; /* Must be at the beginning because we downcast! */
    int ti_nkey;
    rel_time_t ti_ctime;
    rel_time_t ti_atime; /* Time this item was created/last accessed */
    int ti_access_count; /* Int count for number of times key has been accessed */
} topkey_item_t;

class TopKeys {
public:
    TopKeys(int mkeys);
    ~TopKeys();

    void updateKey(const void *key,
                   size_t nkey,
                   rel_time_t operation_time);

    ENGINE_ERROR_CODE stats(const void *cookie,
                            const rel_time_t current_time,
                            ADD_STAT add_stat);

    /**
     * Passing a set of topkeys, and relevant context data will
     * return a cJSON object containing an array of topkeys:
     * {
     *   "topkeys": [
     *      {
     *          "key": "somekey",
     *          "access_count": nnn,
     *          "ctime": ccc,
     *          "atime": aaa
     *      }, ..., { ... }
     *    ]
     * }
     */
    ENGINE_ERROR_CODE json_stats(cJSON *object,
                                 const rel_time_t current_time);


private:
    topkey_item_t *getOrCreate(const void *key,
                               size_t nkey,
                               rel_time_t operation_time);

    void deleteTail();

    int nkeys;
    int max_keys;
    std::mutex mutex;
    dlist_t list;
    std::map<std::string, topkey_item_t*> hash;
};
