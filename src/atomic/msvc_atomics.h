/* -*- Mode: C; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2013 Couchbase, Inc
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

#ifndef SRC_ATOMIC_MSVC_ATOMICS_H
#define SRC_ATOMIC_MSVC_ATOMICS_H 1

// This is currently just a dummy file!

#include "config.h"

#include <memcached/engine.h>

int ep_sync_lock_test_and_set(volatile int *dest, int value);
void ep_sync_lock_release(volatile int *dest);
void ep_sync_synchronize(void);
rel_time_t ep_sync_add_and_fetch(volatile uint64_t *dest, uint64_t value);
rel_time_t ep_sync_add_and_fetch(volatile uint32_t *dest, uint32_t value);
int ep_sync_add_and_fetch(volatile int *dest, int value);
uint8_t ep_sync_add_and_fetch(volatile uint8_t *dest, uint8_t value);
int ep_sync_fetch_and_add(volatile int *dest, int value);
uint64_t ep_sync_fetch_and_add(volatile uint64_t *dest, uint64_t value);
uint32_t ep_sync_fetch_and_add(volatile uint32_t *dest, uint32_t value);
hrtime_t ep_sync_fetch_and_add(volatile hrtime_t *dest, hrtime_t value);
bool ep_sync_bool_compare_and_swap(volatile uint8_t *dest, uint8_t prev, uint8_t next);
bool ep_sync_bool_compare_and_swap(volatile bool *dest, bool prev, bool next);
bool ep_sync_bool_compare_and_swap(volatile int *dest, int prev, int next);
bool ep_sync_bool_compare_and_swap(volatile unsigned int *dest,
    unsigned int prev,
    unsigned int next);
bool ep_sync_bool_compare_and_swap(volatile uint64_t *dest,
    uint64_t prev,
    uint64_t next);

/*
* Unfortunately C++ isn't all that happy about assinging everything to/from a
* void pointer without a cast, so we need to add extra functions.
* Luckily we know that the size_t is big enough to keep a pointer, so
* we can reuse the size_t function for we already defined
*/
class QueuedItem;
typedef std::queue<QueuedItem> ItemQueue;
typedef std::queue<int> IntQueue;
class VBucket;
class VBucketHolder;
class Doodad;
class Blob;
class ConnHandler;

bool ep_sync_bool_compare_and_swap(ItemQueue * volatile *dest, ItemQueue *prev, ItemQueue *next);
bool ep_sync_bool_compare_and_swap(VBucket* volatile* dest, VBucket* prev, VBucket* next);
bool ep_sync_bool_compare_and_swap(Blob* volatile* dest, Blob* prev, Blob* next);
bool ep_sync_bool_compare_and_swap(QueuedItem* volatile* dest, QueuedItem* prev, QueuedItem* next);
bool ep_sync_bool_compare_and_swap(VBucketHolder* volatile* dest, VBucketHolder* prev, VBucketHolder* next);
bool ep_sync_bool_compare_and_swap(Doodad* volatile* dest, Doodad* prev, Doodad* next);
bool ep_sync_bool_compare_and_swap(IntQueue * volatile *dest, IntQueue *prev, IntQueue *next);
bool ep_sync_bool_compare_and_swap(ConnHandler * volatile *dest, ConnHandler *prev, ConnHandler *next);


typedef int pthread_key_t;
int pthread_key_create(pthread_key_t *, void(*)(void*));
int pthread_key_delete(pthread_key_t);
void *pthread_getspecific(pthread_key_t);
int pthread_setspecific(pthread_key_t, const void *);

#endif  // SRC_ATOMIC_MSVC_ATOMICS_H
