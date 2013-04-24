/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2012 Couchbase, Inc
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

#include "couch-kvstore/couch-kvstore-dummy.h"

CouchKVStore::CouchKVStore(EPStats &, Configuration &, bool) : KVStore()
{
    throw std::runtime_error("This kvstore should never be used");
}

CouchKVStore::CouchKVStore(const CouchKVStore &) : KVStore()
{
    throw std::runtime_error("This kvstore should never be used");
}

void CouchKVStore::reset()
{
    throw std::runtime_error("This kvstore should never be used");
}

bool CouchKVStore::begin()
{
    throw std::runtime_error("This kvstore should never be used");
}

void CouchKVStore::rollback()
{
    throw std::runtime_error("This kvstore should never be used");
}

void CouchKVStore::set(const Item &, Callback<mutation_result> &)
{
    throw std::runtime_error("This kvstore should never be used");
}

void CouchKVStore::get(const std::string &, uint64_t, uint16_t,
                       Callback<GetValue> &)
{
    throw std::runtime_error("This kvstore should never be used");
}

void CouchKVStore::del(const Item &, uint64_t, Callback<int> &)
{
    throw std::runtime_error("This kvstore should never be used");
}

bool CouchKVStore::delVBucket(uint16_t)
{
    throw std::runtime_error("This kvstore should never be used");
}

vbucket_map_t CouchKVStore::listPersistedVbuckets()
{
    throw std::runtime_error("This kvstore should never be used");
}


bool CouchKVStore::snapshotVBuckets(const vbucket_map_t &)
{
    throw std::runtime_error("This kvstore should never be used");
}

bool CouchKVStore::snapshotStats(const std::map<std::string, std::string> &)
{
    // noop, virtual function implementation for abstract base class
    return true;
}

void CouchKVStore::dump(shared_ptr<Callback<GetValue> >)
{
    throw std::runtime_error("This kvstore should never be used");
}

void CouchKVStore::dump(uint16_t, shared_ptr<Callback<GetValue> >)
{
    throw std::runtime_error("This kvstore should never be used");
}

StorageProperties CouchKVStore::getStorageProperties()
{
    throw std::runtime_error("This kvstore should never be used");
}

bool CouchKVStore::commit(void)
{
    throw std::runtime_error("This kvstore should never be used");
}

