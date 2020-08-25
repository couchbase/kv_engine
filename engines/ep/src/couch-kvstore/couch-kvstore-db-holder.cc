/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2020 Couchbase, Inc
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

#include "couch-kvstore-db-holder.h"

#include "couch-kvstore.h"

DbHolder::DbHolder(DbHolder&& other) : DbHolder(other.kvstore) {
    db = other.releaseDb();
    fileRev = other.fileRev;
}

DbHolder& DbHolder::operator=(DbHolder&& other) {
    db = other.releaseDb();
    fileRev = other.fileRev;
    return *this;
}

void DbHolder::close() {
    if (db) {
        kvstore.get().closeDatabaseHandle(releaseDb());
    }
}
