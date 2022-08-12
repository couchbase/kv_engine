/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2017-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */

#include "doc_pre_expiry.h"
#include <memcached/protocol_binary.h>
#include <xattr/blob.h>

std::string document_pre_expiry(const item_info& itm_info) {
    if (!cb::mcbp::datatype::is_xattr(itm_info.datatype)) {
        // The object does not contain any XATTRs so we should remove
        // the entire content
        return {};
    }

    // Operate on a copy
    cb::char_buffer payload{static_cast<char*>(itm_info.value[0].iov_base),
                            itm_info.value[0].iov_len};
    cb::xattr::Blob originalBlob(
            payload, cb::mcbp::datatype::is_snappy(itm_info.datatype));
    auto copy = cb::xattr::Blob(originalBlob);
    copy.prune_user_keys();
    const auto final = copy.finalize();

    if (final.size() == 0) {
        // The old payload only contained user xattrs and
        // we removed everything
        return {};
    }

    return {final.data(), final.size()};
}
