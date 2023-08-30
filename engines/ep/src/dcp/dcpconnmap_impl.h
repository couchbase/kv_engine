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

#pragma once

#include "dcpconnmap.h"

#include "conn_store.h"

/**
 * Separate implementation file for DcpConnMap templated functions. We don't
 * want to include conn_store.h everywhere as it's a pretty big header.
 */
template <typename Fun>
void DcpConnMap::each(Fun&& f) {
    // We want to minimise how long cookieToConnMap is locked for - i.e.
    // we don't want to run `f` for each item while locked.
    // As such, take a copy of the map (under shared lock) then iterate that
    // copy.
    auto copy = std::as_const(*connStore)
                        .getCookieToConnectionMapHandle()
                        ->copyCookieToConn();
    for (auto& c : copy) {
        f(c.second);
    }
}
