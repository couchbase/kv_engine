/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2022-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */
#include "ep_engine_storage.h"
#include "objectregistry.h"

EPEngineStorageBase::EPEngineStorageBase()
    : owner(ObjectRegistry::getCurrentEngine()) {
    // TODO: Assert that owner != nullptr once all tests support that
}

void EPEngineStorageBase::deallocate() const {
    BucketAllocationGuard guard(owner);
    delete this;
}