/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2020-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
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
