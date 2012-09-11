/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
#include "config.h"
#include "couch-kvstore/couch-kvstore-dummy.hh"

CouchKVStore::CouchKVStore(EventuallyPersistentEngine &, bool) : KVStore()
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

