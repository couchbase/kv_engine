/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2011 Couchbase, Inc.
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
#include "ep_engine.h"

static ThreadLocal<EventuallyPersistentEngine*> *th;
static ThreadLocal<Atomic<size_t>*> *initial_track;

/**
 * Object registry link hook for getting the registry thread local
 * installed.
 */
class installer {
public:
   installer() {
      if (th == NULL) {
         th = new ThreadLocal<EventuallyPersistentEngine*>();
         initial_track = new ThreadLocal<Atomic<size_t>*>();
      }
   }
} install;

static bool verifyEngine(EventuallyPersistentEngine *engine)
{
   if (engine == NULL) {
       if (getenv("ALLOW_NO_STATS_UPDATE") != NULL) {
           return false;
       } else {
           assert(engine);
       }
   }
   return true;
}


void ObjectRegistry::onCreateBlob(Blob *blob)
{
   EventuallyPersistentEngine *engine = th->get();
   if (verifyEngine(engine)) {
       EPStats &stats = engine->getEpStats();
       stats.currentSize.incr(blob->getSize());
       stats.totalValueSize.incr(blob->getSize());
       assert(stats.currentSize.get() < GIGANTOR);
   }
}

void ObjectRegistry::onDeleteBlob(Blob *blob)
{
   EventuallyPersistentEngine *engine = th->get();
   if (verifyEngine(engine)) {
       EPStats &stats = engine->getEpStats();
       stats.currentSize.decr(blob->getSize());
       stats.totalValueSize.decr(blob->getSize());
       assert(stats.currentSize.get() < GIGANTOR);
   }
}

void ObjectRegistry::onCreateQueuedItem(QueuedItem *qi)
{
   EventuallyPersistentEngine *engine = th->get();
   if (verifyEngine(engine)) {
       EPStats &stats = engine->getEpStats();
       stats.memOverhead.incr(qi->size());
       assert(stats.memOverhead.get() < GIGANTOR);
   }
}

void ObjectRegistry::onDeleteQueuedItem(QueuedItem *qi)
{
   EventuallyPersistentEngine *engine = th->get();
   if (verifyEngine(engine)) {
       EPStats &stats = engine->getEpStats();
       stats.memOverhead.decr(qi->size());
       assert(stats.memOverhead.get() < GIGANTOR);
   }
}

void ObjectRegistry::onCreateItem(Item *pItem)
{
   EventuallyPersistentEngine *engine = th->get();
   if (verifyEngine(engine)) {
       EPStats &stats = engine->getEpStats();
       stats.memOverhead.incr(pItem->size() - pItem->getValMemSize());
       assert(stats.memOverhead.get() < GIGANTOR);
   }
}

void ObjectRegistry::onDeleteItem(Item *pItem)
{
   EventuallyPersistentEngine *engine = th->get();
   if (verifyEngine(engine)) {
       EPStats &stats = engine->getEpStats();
       stats.memOverhead.decr(pItem->size() - pItem->getValMemSize());
       assert(stats.memOverhead.get() < GIGANTOR);
   }
}

EventuallyPersistentEngine *ObjectRegistry::getCurrentEngine() {
    return th->get();
}

EventuallyPersistentEngine *ObjectRegistry::onSwitchThread(EventuallyPersistentEngine *engine,
                                                           bool want_old_thread_local)
{
    EventuallyPersistentEngine *old_engine = NULL;
    if (want_old_thread_local) {
        old_engine = th->get();
    }
    th->set(engine);
    return old_engine;
}

void ObjectRegistry::setStats(Atomic<size_t>* init_track) {
    initial_track->set(init_track);
}

bool ObjectRegistry::memoryAllocated(size_t mem) {
    EventuallyPersistentEngine *engine = th->get();
    if (initial_track->get()) {
        initial_track->get()->incr(mem);
    }
    if (!engine) {
        return false;
    }
    EPStats &stats = engine->getEpStats();
    stats.totalMemory.incr(mem);
    if (stats.memoryTrackerEnabled && stats.totalMemory.get() >= GIGANTOR) {
        LOG(EXTENSION_LOG_WARNING,
            "Total memory in memoryAllocated() >= GIGANTOR !!! "
            "Disable the memory tracker...\n");
        stats.memoryTrackerEnabled.set(false);
    }
    return true;
}

bool ObjectRegistry::memoryDeallocated(size_t mem) {
    EventuallyPersistentEngine *engine = th->get();
    if (initial_track->get()) {
        initial_track->get()->decr(mem);
    }
    if (!engine) {
        return false;
    }
    EPStats &stats = engine->getEpStats();
    stats.totalMemory.decr(mem);
    if (stats.memoryTrackerEnabled && stats.totalMemory.get() >= GIGANTOR) {
        LOG(EXTENSION_LOG_WARNING,
            "Total memory in memoryDeallocated() >= GIGANTOR !!! "
            "Disable the memory tracker...\n");
        stats.memoryTrackerEnabled.set(false);
    }
    return true;
}
