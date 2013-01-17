/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2011 Couchbase, Inc
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

#include <string.h>

#include <algorithm>
#include <cctype>
#include <cstdlib>

#include "blackhole-kvstore/blackhole.h"

BlackholeKVStore::BlackholeKVStore(bool)
{
    // Empty
}

BlackholeKVStore::~BlackholeKVStore()
{
    // Empty
}


void BlackholeKVStore::reset()
{
    // Empty
}

void BlackholeKVStore::set(const Item &itm,
                           Callback<mutation_result> &cb)
{
    int cr = (itm.getId() <= 0) ? 1 : 0;
    mutation_result p(1, cr);
    cb.callback(p);
}

void BlackholeKVStore::get(const std::string &,
                           uint64_t,
                           uint16_t,
                           Callback<GetValue> &cb)
{
    GetValue rv;
    cb.callback(rv);
}


void BlackholeKVStore::del(const Item &,
                           uint64_t,
                           Callback<int> &cb)
{
    int val = 0;
    cb.callback(val);
}

bool BlackholeKVStore::delVBucket(uint16_t, bool)
{
    return true;
}

vbucket_map_t BlackholeKVStore::listPersistedVbuckets()
{
    std::map<uint16_t, vbucket_state> rv;
    return rv;
}

bool BlackholeKVStore::snapshotVBuckets(const vbucket_map_t &)
{
    return true;
}

bool BlackholeKVStore::snapshotStats(const std::map<std::string, std::string> &)
{
    return true;
}

void BlackholeKVStore::dump(shared_ptr<Callback<GetValue> >)
{
}

void BlackholeKVStore::dump(uint16_t, shared_ptr<Callback<GetValue> >)
{
}

StorageProperties BlackholeKVStore::getStorageProperties()
{
    size_t concurrency(10);
    StorageProperties rv(concurrency, concurrency - 1, 1, true, true,
                         true, false);
    return rv;
}

bool BlackholeKVStore::commit(void)
{
    return true;
}

bool BlackholeKVStore::begin(void)
{
    return true;
}

void BlackholeKVStore::rollback(void)
{
}
