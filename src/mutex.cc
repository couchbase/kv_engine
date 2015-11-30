/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2015 Couchbase, Inc.
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
#include "mutex.h"

Mutex::Mutex()
{
    cb_mutex_initialize(&mutex);
}

Mutex::~Mutex() {
    cb_mutex_destroy(&mutex);
}

void Mutex::lock() {
    cb_mutex_enter(&mutex);
}

bool Mutex::try_lock() {
    if (!cb_mutex_try_enter(&mutex)) {
        return true;
    }
    return false;
}

void Mutex::unlock() {
    cb_mutex_exit(&mutex);
}
