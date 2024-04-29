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

#include "serialised_dockey.h"

#include <mcbp/protocol/unsigned_leb128.h>

#include <ostream>

CollectionID SerialisedDocKey::getCollectionID() const {
    return cb::mcbp::unsigned_leb128<CollectionIDType>::decode({data(), size()})
            .first;
}

bool SerialisedDocKey::isInSystemEventCollection() const {
    return data()[0] == CollectionID::SystemEvent;
}

bool SerialisedDocKey::isInDefaultCollection() const {
    return data()[0] == CollectionID::Default;
}

bool SerialisedDocKey::operator==(const DocKey& rhs) const {
    // Does 'rhs' encode a collection-ID
    if (rhs.getEncoding() == DocKeyEncodesCollectionId::Yes) {
        // compare size and then the entire key
        return rhs.size() == size() &&
               std::equal(rhs.begin(), rhs.end(), data());
    }

    // 'rhs' does not encode a collection - it is therefore a key in the default
    // collection. We can check byte 0 of this key to see if this key is also
    // in the default collection. if so compare sizes (discounting the
    // default collection prefix) and finally the key data.
    return data()[0] == CollectionID::Default && rhs.size() == size() - 1 &&
           std::equal(rhs.begin(), rhs.end(), data() + 1);
}

bool SerialisedDocKey::operator==(const SerialisedDocKey& rhs) const {
    return rhs.size() == size() && std::memcmp(data(), rhs.data(), size()) == 0;
}

SerialisedDocKey::SerialisedDocKey(cb::const_byte_buffer key,
                                   CollectionID cid) {
    cb::mcbp::unsigned_leb128<CollectionIDType> leb128(uint32_t{cid});
    length = gsl::narrow_cast<uint8_t>(key.size() + leb128.size());
    std::copy(key.begin(),
              key.end(),
              std::copy(leb128.begin(), leb128.end(), &bytes[0]));
}

std::ostream& operator<<(std::ostream& os, const SerialisedDocKey& key) {
    os << key.to_string() << ", size:" << std::dec << key.size();
    return os;
}
