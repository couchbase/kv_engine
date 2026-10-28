/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2016-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */

#include "kvstore_iface.h"
#include "item.h"

KVStoreIface::CreateItemCB KVStoreIface::getDefaultCreateItemCallback() {
    return [](const DocKeyView& key,
              size_t nbytes,
              uint32_t flags,
              time_t exptime,
              const value_t& body,
              protocol_binary_datatype_t datatype,
              uint64_t theCas,
              int64_t bySeq,
              Vbid vbid,
              uint64_t revSeq) {
        auto item = std::make_unique<Item>(key,
                                           flags,
                                           exptime,
                                           body,
                                           datatype,
                                           theCas,
                                           bySeq,
                                           vbid,
                                           revSeq);
        return std::make_pair(cb::engine_errc::success, std::move(item));
    };
}

std::vector<std::unique_ptr<KVFileHandle>> KVStoreIface::makeFileHandles(
        Vbid vbid, size_t count) const {
    Expects(count > 0);
    std::vector<std::unique_ptr<KVFileHandle>> handles;
    handles.reserve(count);
    for (size_t idx = 0; idx < count; ++idx) {
        auto handle = makeFileHandle(vbid);
        if (!handle) {
            return {};
        }
        handles.push_back(std::move(handle));
    }
    return handles;
}
