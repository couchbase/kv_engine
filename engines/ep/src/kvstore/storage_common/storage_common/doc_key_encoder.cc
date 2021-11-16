/*
 *   Copyright 2021-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */

#include "doc_key_encoder.h"

#include <memcached/dockey.h>
#include <memcached/systemevent.h>

std::string encodeDocKey(std::string_view key,
                         std::string_view collection,
                         uint32_t keyNamespace) {
    std::string ret;

    if (keyNamespace != 0) {
        auto leb128 = cb::mcbp::unsigned_leb128<uint64_t>(keyNamespace);
        ret.append(leb128.begin(), leb128.end());

        if (keyNamespace == CollectionID::System) {
            if (key == "_collection") {
                auto leb128 = cb::mcbp::unsigned_leb128<uint32_t>(
                        static_cast<uint32_t>(SystemEvent::Collection));
                ret.append(leb128.begin(), leb128.end());
            } else if (key == "_scope") {
                auto leb128 = cb::mcbp::unsigned_leb128<uint32_t>(
                        static_cast<uint32_t>(SystemEvent::Scope));
                ret.append(leb128.begin(), leb128.end());
            }
        }
    }

    auto numericCollection = 0;
    if (!collection.empty()) {
        numericCollection = stoull(std::string(collection));
    }
    CollectionID cid(numericCollection);

    ret.append(DocKey::makeWireEncodedString(cid, std::string(key)));

    return ret;
}