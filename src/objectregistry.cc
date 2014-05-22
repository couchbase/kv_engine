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

#include "threadlocal.h"
#include "ep_engine.h"
#include "objectregistry.h"
#include "stored-value.h"

static ThreadLocal<EventuallyPersistentEngine*> *th;
static ThreadLocal<AtomicValue<size_t>*> *initial_track;

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
         initial_track = new ThreadLocal<AtomicValue<size_t>*>();
      }
   }
} install;

static bool verifyEngine(EventuallyPersistentEngine *engine)
{
   if (engine == NULL) {
       if (getenv("ALLOW_NO_STATS_UPDATE") != NULL) {
           return false;
       } else {
           cb_assert(engine);
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
       cb_assert(stats.currentSize.load() < GIGANTOR);
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
       cb_assert(stats.currentSize.load() < GIGANTOR);
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
       cb_assert(stats.currentSize.load() < GIGANTOR);
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
       cb_assert(stats.currentSize.load() < GIGANTOR);
   }
}


void ObjectRegistry::onCreateItem(const Item *pItem)
{
   EventuallyPersistentEngine *engine = th->get();
   if (verifyEngine(engine)) {
       EPStats &stats = engine->getEpStats();
       stats.memOverhead.fetch_add(pItem->size() - pItem->getValMemSize());
       cb_assert(stats.memOverhead.load() < GIGANTOR);
   }
}

void ObjectRegistry::onDeleteItem(const Item *pItem)
{
   EventuallyPersistentEngine *engine = th->get();
   if (verifyEngine(engine)) {
       EPStats &stats = engine->getEpStats();
       stats.memOverhead.fetch_sub(pItem->size() - pItem->getValMemSize());
       cb_assert(stats.memOverhead.load() < GIGANTOR);
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

void ObjectRegistry::setStats(AtomicValue<size_t>* init_track) {
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
    stats.totalMemory.fetch_add(mem);
    if (stats.memoryTrackerEnabled && stats.totalMemory.load() >= GIGANTOR) {
        LOG(EXTENSION_LOG_WARNING,
            "Total memory in memoryAllocated() >= GIGANTOR !!! "
            "Disable the memory tracker...\n");
        stats.memoryTrackerEnabled.store(false);
    }
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
    stats.totalMemory.fetch_sub(mem);
    if (stats.memoryTrackerEnabled && stats.totalMemory.load() >= GIGANTOR) {
        EXTENSION_LOG_LEVEL logSeverity = EXTENSION_LOG_WARNING;
        if (stats.isShutdown && !stats.forceShutdown) {
            logSeverity = EXTENSION_LOG_INFO;
        }
        LOG(logSeverity,
            "Total memory in memoryDeallocated() >= GIGANTOR !!! "
            "Disable the memory tracker...\n");
        stats.memoryTrackerEnabled.store(false);
    }
    return true;
}
