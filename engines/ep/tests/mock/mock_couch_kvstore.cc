/*
 *     Copyright 2022-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */

#pragma once

#include "mock_couch_kvstore.h"

#include "kvstore/couch-kvstore/couch-kvstore-db-holder.h"

bool MockCouchKVStore::deleteLocalDoc(Vbid vbid, std::string_view doc) {
    PendingLocalDocRequestQueue pendingLocalReqsQ;
    pendingLocalReqsQ.emplace_back(std::string(doc),
                                   CouchLocalDocRequest::IsDeleted{});

    DbHolder db(*this);
    auto options = COUCHSTORE_OPEN_FLAG_CREATE;
    auto errCode = openDB(vbid, db, options);
    if (errCode) {
        return false;
    }

    updateLocalDocuments(*db, pendingLocalReqsQ);

    errCode = couchstore_commit(db,
                                [](const std::system_error&) { return true; });

    return !errCode;
}