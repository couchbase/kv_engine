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
#ifndef OBJECTREGISTRY_HH
#define OBJECTREGISTRY_HH 1

class EventuallyPersistentEngine;
class Blob;
class QueuedItem;

class ObjectRegistry {
public:
    static void onCreateBlob(Blob *blob);
    static void onDeleteBlob(Blob *blob);

    static void onCreateQueuedItem(QueuedItem *qi);
    static void onDeleteQueuedItem(QueuedItem *qi);

    static void onCreateItem(Item *pItem);
    static void onDeleteItem(Item *pItem);

    static EventuallyPersistentEngine *onSwitchThread(EventuallyPersistentEngine *engine,
                                                      bool want_old_thread_local = false);

    static void setStats(Atomic<size_t>* init_track);
    static bool memoryAllocated(size_t mem);
    static bool memoryDeallocated(size_t mem);
};

#endif
