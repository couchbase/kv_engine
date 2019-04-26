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

#pragma once

#include "dcpconnmap.h"

#include "conn_store.h"

/**
 * Separate implementation file for DcpConnMap templated functions. We don't
 * want to include conn_store.h everywhere as it's a pretty big header.
 */
template <typename Fun>
void DcpConnMap::each(Fun f) {
    // Hold the handle to keep the lock during iteration
    auto handle = connStore->getCookieToConnectionMapHandle();
    for (auto& c : *handle) {
        f(c.second);
    }
}
