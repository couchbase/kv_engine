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

#include "config.h"

#include "objectregistry.h"

#include "threadlocal.h"
#include "ep_engine.h"
#include "stored-value.h"

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
       if (getenv("ALLOW_NO_STATS_UPDATE") != NULL) {
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


void ObjectRegistry::onCreateBlob(const Blob *blob)
{
   EventuallyPersistentEngine *engine = th->get();
   if (verifyEngine(engine)) {
       EPStats &stats = engine->getEpStats();
       size_t size = getAllocSize(blob);
       if (size == 0) {
           size = blob->getSize();
       } else {
           stats.blobOverhead.fetch_add(size - blob->getSize());
       }
       stats.currentSize.fetch_add(size);
       stats.totalValueSize.fetch_add(size);
       stats.numBlob++;
   }
}

void ObjectRegistry::onDeleteBlob(const Blob *blob)
{
   EventuallyPersistentEngine *engine = th->get();
   if (verifyEngine(engine)) {
       EPStats &stats = engine->getEpStats();
       size_t size = getAllocSize(blob);
       if (size == 0) {
           size = blob->getSize();
       } else {
           stats.blobOverhead.fetch_sub(size - blob->getSize());
       }
       stats.currentSize.fetch_sub(size);
       stats.totalValueSize.fetch_sub(size);
       stats.numBlob--;
   }
}

void ObjectRegistry::onCreateStoredValue(const StoredValue *sv)
{
   EventuallyPersistentEngine *engine = th->get();
   if (verifyEngine(engine)) {
       EPStats &stats = engine->getEpStats();
       size_t size = getAllocSize(sv);
       if (size == 0) {
           size = sv->getObjectSize();
       } else {
           stats.storedValOverhead.fetch_add(size - sv->getObjectSize());
       }
       stats.numStoredVal++;
       stats.totalStoredValSize.fetch_add(size);
   }
}

void ObjectRegistry::onDeleteStoredValue(const StoredValue *sv)
{
   EventuallyPersistentEngine *engine = th->get();
   if (verifyEngine(engine)) {
       EPStats &stats = engine->getEpStats();
       size_t size = getAllocSize(sv);
       if (size == 0) {
           size = sv->getObjectSize();
       } else {
           stats.storedValOverhead.fetch_sub(size - sv->getObjectSize());
       }
       stats.totalStoredValSize.fetch_sub(size);
       stats.numStoredVal--;
   }
}


void ObjectRegistry::onCreateItem(const Item *pItem)
{
   EventuallyPersistentEngine *engine = th->get();
   if (verifyEngine(engine)) {
       EPStats &stats = engine->getEpStats();
       stats.memOverhead->fetch_add(pItem->size() - pItem->getValMemSize());
       ++(*stats.numItem);
   }
}

void ObjectRegistry::onDeleteItem(const Item *pItem)
{
   EventuallyPersistentEngine *engine = th->get();
   if (verifyEngine(engine)) {
       EPStats &stats = engine->getEpStats();
       stats.memOverhead->fetch_sub(pItem->size() - pItem->getValMemSize());
       ++(*stats.numItem);
   }
}

EventuallyPersistentEngine *ObjectRegistry::getCurrentEngine() {
    return th->get();
}

EventuallyPersistentEngine *ObjectRegistry::onSwitchThread(
                                            EventuallyPersistentEngine *engine,
                                            bool want_old_thread_local)
{
    EventuallyPersistentEngine *old_engine = NULL;
    if (want_old_thread_local) {
        old_engine = th->get();
    }
    th->set(engine);
    return old_engine;
}

void ObjectRegistry::setStats(std::atomic<size_t>* init_track) {
    initial_track->set(init_track);
}

bool ObjectRegistry::memoryAllocated(size_t mem) {
    EventuallyPersistentEngine *engine = th->get();
    if (initial_track->get()) {
        initial_track->get()->fetch_add(mem);
    }
    if (!engine) {
        return false;
    }
    EPStats &stats = engine->getEpStats();
    stats.totalMemory->fetch_add(mem);
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
    stats.totalMemory->fetch_sub(mem);
    return true;
}
#endif
