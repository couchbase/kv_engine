/*
 *     Copyright 2022-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */

#include <memcached/cookie_iface.h>

const cb::EngineStorageIface* CookieIface::getEngineStorage() const {
    return engine_storage.lock()->get();
}

cb::unique_engine_storage_ptr CookieIface::takeEngineStorage() {
    cb::unique_engine_storage_ptr p;
    engine_storage.swap(p);
    return p;
}

void CookieIface::setEngineStorage(cb::unique_engine_storage_ptr value) {
    // Swap so we don't destroy the object while holding the lock.
    engine_storage.swap(value);
}