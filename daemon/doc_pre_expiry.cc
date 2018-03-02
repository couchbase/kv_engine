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
#include <platform/sized_buffer.h>
#include <xattr/blob.h>
#include <xattr/utils.h>

bool document_pre_expiry(item_info& itm_info) {
    if (!mcbp::datatype::is_xattr(itm_info.datatype)) {
        // The object does not contain any XATTRs so we should remove
        // the entire content
        return false;
    }

    const auto xattr_size = cb::xattr::get_body_offset(
        {static_cast<const char*>(itm_info.value[0].iov_base),
         itm_info.value[0].iov_len});

    cb::char_buffer payload{static_cast<char*>(itm_info.value[0].iov_base),
                            xattr_size};

    cb::xattr::Blob blob(payload);
    blob.prune_user_keys();
    auto pruned = blob.finalize();
    if (pruned.len == 0) {
        // The old payload only contained user xattrs and
        // we removed everything
        return false;
    }

    // Pruning the user keys should just repack the data internally
    // without any allocations, but to be on the safe side we should
    // just check to verify, and if it happened to have done any
    // reallocations we could copy the data into our buffer if it fits.
    // (or we could throw an exception here, but I'd hate to get that
    // 2AM call if we can avoid that with a simple fallback check)
    if (pruned.buf != payload.buf) {
        if (pruned.len > payload.len) {
           throw std::logic_error("pre_expiry_document: the pruned object "
                                  "won't fit!");
        }

        std::copy(pruned.buf, pruned.buf + pruned.len, payload.buf);
    }

    // Update the length field of the item
    itm_info.nbytes = pruned.len;
    itm_info.value[0].iov_len = pruned.len;
    // Clear all other datatype flags (we've stripped off everything)
    itm_info.datatype = PROTOCOL_BINARY_DATATYPE_XATTR;

    return true;
}
