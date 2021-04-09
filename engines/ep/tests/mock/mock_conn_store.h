/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2019-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */

#pragma once

#include "conn_store.h"

#include <memcached/vbucket.h>

class MockConnStore : public ConnStore {
public:
    explicit MockConnStore(EventuallyPersistentEngine& engine)
        : ConnStore(engine) {
    }

    VBToConnsMap& getVBToConnsMap() {
        return vbToConns;
    }

    VBToConnsMap::value_type::iterator getVBToConnsItr(
            Vbid vbid, const ConnHandler& conn) {
        size_t lock_num = vbid.get() % vbConnLocks.size();
        std::unique_lock<std::mutex> lh(vbConnLocks[lock_num]);

        return ConnStore::getVBToConnsItr(lh, vbid, conn);
    }
};
