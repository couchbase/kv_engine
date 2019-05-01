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

#include "objectregistry.h"

#include "ep_engine.h"
#include "item.h"
#include "stored-value.h"
#include "threadlocal.h"
#include <platform/cb_arena_malloc.h>

#if 1
static ThreadLocal<EventuallyPersistentEngine*> *th;
static ThreadLocal<std::atomic<size_t>*> *initial_track;

extern "C" {
    static size_t defaultGetAllocSize(const void *) {
        return 0;
    }
}

static get_allocation_size getAllocSize = defaultGetAllocSize;



/**
 * Object registry link hook for getting the registry thread local
 * installed.
 */
class installer {
public:
   installer() {
      if (th == NULL) {
         th = new ThreadLocal<EventuallyPersistentEngine*>();
         initial_track = new ThreadLocal<std::atomic<size_t>*>();
      }
   }

   ~installer() {
       delete initial_track;
       delete th;
   }
} install;

static bool verifyEngine(EventuallyPersistentEngine *engine)
{
   if (engine == NULL) {
       static const char* allowNoStatsUpdate = getenv("ALLOW_NO_STATS_UPDATE");
       if (allowNoStatsUpdate) {
           return false;
       } else {
           throw std::logic_error("verifyEngine: engine should be non-NULL");
       }
   }
   return true;
}

void ObjectRegistry::initialize(get_allocation_size func) {
    getAllocSize = func;
}

void ObjectRegistry::reset() {
    getAllocSize = defaultGetAllocSize;
}

void ObjectRegistry::onCreateBlob(const Blob *blob)
{
   EventuallyPersistentEngine *engine = th->get();
   if (verifyEngine(engine)) {
       auto& coreLocalStats = engine->getEpStats().coreLocal.get();

       size_t size = getAllocSize(blob);
       if (size == 0) {
           size = blob->getSize();
       } else {
           coreLocalStats->blobOverhead.fetch_add(size - blob->getSize());
       }
       coreLocalStats->currentSize.fetch_add(size);
       coreLocalStats->totalValueSize.fetch_add(size);
       coreLocalStats->numBlob++;
   }
}

void ObjectRegistry::onDeleteBlob(const Blob *blob)
{
   EventuallyPersistentEngine *engine = th->get();
   if (verifyEngine(engine)) {
       auto& coreLocalStats = engine->getEpStats().coreLocal.get();

       size_t size = getAllocSize(blob);
       if (size == 0) {
           size = blob->getSize();
       } else {
           coreLocalStats->blobOverhead.fetch_sub(size - blob->getSize());
       }
       coreLocalStats->currentSize.fetch_sub(size);
       coreLocalStats->totalValueSize.fetch_sub(size);
       coreLocalStats->numBlob--;
   }
}

void ObjectRegistry::onCreateStoredValue(const StoredValue *sv)
{
   EventuallyPersistentEngine *engine = th->get();
   if (verifyEngine(engine)) {
       auto& coreLocalStats = engine->getEpStats().coreLocal.get();

       size_t size = getAllocSize(sv);
       if (size == 0) {
           size = sv->getObjectSize();
       }
       coreLocalStats->numStoredVal++;
       coreLocalStats->totalStoredValSize.fetch_add(size);
   }
}

void ObjectRegistry::onDeleteStoredValue(const StoredValue *sv)
{
   EventuallyPersistentEngine *engine = th->get();
   if (verifyEngine(engine)) {
       auto& coreLocalStats = engine->getEpStats().coreLocal.get();

       size_t size = getAllocSize(sv);
       if (size == 0) {
           size = sv->getObjectSize();
       }
       coreLocalStats->totalStoredValSize.fetch_sub(size);
       coreLocalStats->numStoredVal--;
   }
}


void ObjectRegistry::onCreateItem(const Item *pItem)
{
   EventuallyPersistentEngine *engine = th->get();
   if (verifyEngine(engine)) {
       auto& coreLocalStats = engine->getEpStats().coreLocal.get();
       coreLocalStats->memOverhead.fetch_add(pItem->size() -
                                             pItem->getValMemSize());
       ++coreLocalStats->numItem;
   }
}

void ObjectRegistry::onDeleteItem(const Item *pItem)
{
   EventuallyPersistentEngine *engine = th->get();
   if (verifyEngine(engine)) {
       auto& coreLocalStats = engine->getEpStats().coreLocal.get();
       coreLocalStats->memOverhead.fetch_sub(pItem->size() -
                                             pItem->getValMemSize());
       --coreLocalStats->numItem;
   }
}

EventuallyPersistentEngine *ObjectRegistry::getCurrentEngine() {
    return th->get();
}

EventuallyPersistentEngine *ObjectRegistry::onSwitchThread(
                                            EventuallyPersistentEngine *engine,
                                            bool want_old_thread_local) {
    EventuallyPersistentEngine *old_engine = NULL;

    if (want_old_thread_local) {
        old_engine = th->get();
    }

    // Set the engine so that onDeleteItem etc... can update their stats
    th->set(engine);

    // Next tell ArenaMalloc what todo so that we can account memory to the
    // bucket
    if (engine) {
        cb::ArenaMalloc::switchToClient(engine->getArenaMallocClient());
    } else {
        cb::ArenaMalloc::switchFromClient();
    }
    return old_engine;
}

// @todo remove the initial_track and associated code, no longer used.
void ObjectRegistry::setStats(std::atomic<size_t>* init_track) {
    initial_track->set(init_track);
}

// @todo remove the memory hooks and associated code, no longer called.
bool ObjectRegistry::memoryAllocated(size_t mem) {
    EventuallyPersistentEngine *engine = th->get();
    if (initial_track->get()) {
        initial_track->get()->fetch_add(mem);
    }
    if (!engine) {
        return false;
    }
    EPStats &stats = engine->getEpStats();
    stats.memAllocated(mem);
    return true;
}

bool ObjectRegistry::memoryDeallocated(size_t mem) {
    EventuallyPersistentEngine *engine = th->get();
    if (initial_track->get()) {
        initial_track->get()->fetch_sub(mem);
    }
    if (!engine) {
        return false;
    }
    EPStats &stats = engine->getEpStats();
    stats.memDeallocated(mem);
    return true;
}

NonBucketAllocationGuard::NonBucketAllocationGuard()
    : engine(ObjectRegistry::onSwitchThread(nullptr, true)) {
}

NonBucketAllocationGuard::~NonBucketAllocationGuard() {
    ObjectRegistry::onSwitchThread(engine);
}

BucketAllocationGuard::BucketAllocationGuard(EventuallyPersistentEngine* engine)
    : previous(ObjectRegistry::onSwitchThread(engine, true)) {
}

BucketAllocationGuard::~BucketAllocationGuard() {
    ObjectRegistry::onSwitchThread(previous);
}

#endif
