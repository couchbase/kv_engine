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

std::string document_pre_expiry(std::string_view view, uint8_t datatype) {
    if (!cb::mcbp::datatype::is_xattr(datatype)) {
        // The object does not contain any XATTRs so we should remove
        // the entire content
        return {};
    }

    // Operate on a copy
    cb::char_buffer payload{const_cast<char*>(view.data()), view.size()};
    cb::xattr::Blob originalBlob(payload,
                                 cb::mcbp::datatype::is_snappy(datatype));
    auto copy = cb::xattr::Blob(originalBlob);
    copy.prune_user_keys();
    const auto final = copy.finalize();

    if (final.empty()) {
        // The old payload only contained user xattrs and
        // we removed everything
        return {};
    }

    return {final.data(), final.size()};
}
