/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2017 Couchbase, Inc.
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

#include "doc_pre_expiry.h"
#include <memcached/protocol_binary.h>
#include <xattr/blob.h>

std::string document_pre_expiry(const item_info& itm_info) {
    if (!mcbp::datatype::is_xattr(itm_info.datatype)) {
        // The object does not contain any XATTRs so we should remove
        // the entire content
        return {};
    }

    // Operate on a copy
    cb::char_buffer payload{static_cast<char*>(itm_info.value[0].iov_base),
                            itm_info.value[0].iov_len};
    cb::xattr::Blob originalBlob(payload,
                                 mcbp::datatype::is_snappy(itm_info.datatype));
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
