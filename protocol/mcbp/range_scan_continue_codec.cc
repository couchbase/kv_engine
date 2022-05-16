/*
 *     Copyright 2022-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */

#include <mcbp/codec/range_scan_continue_codec.h>

#include <mcbp/protocol/unsigned_leb128.h>
#include <memcached/types.h>

namespace cb::mcbp::response {

RangeScanContinueKeyPayload::RangeScanContinueKeyPayload(
        std::string_view payload)
    : payload(payload) {
    // Smallest payload would be 1byte leb128 and 1byte key
    if (payload.size() < 2) {
        throw std::invalid_argument(
                "RangeScanContinueKeyPayload payload too small to decode");
    }
}

std::string_view RangeScanContinueKeyPayload::next() {
    if (payload.empty()) {
        return {};
    }

    cb::const_byte_buffer data(reinterpret_cast<const uint8_t*>(payload.data()),
                               payload.size());

    auto length = cb::mcbp::unsigned_leb128<size_t>::decode(data);

    // Should not have zero
    Expects(length.first);

    // We have a key to return
    std::string_view key(reinterpret_cast<const char*>(length.second.data()),
                         length.first);

    // Now advance the payload for next
    payload = std::string_view{
            reinterpret_cast<const char*>(length.second.data()) + length.first,
            length.second.size() - length.first};

    return key;
}

void RangeScanContinueKeyPayload::encode(std::vector<uint8_t>& v,
                                         const DocKey& key) {
    auto strippedKey = key.makeDocKeyWithoutCollectionID();
    cb::mcbp::unsigned_leb128<size_t> lebSize(strippedKey.size());
    std::copy(lebSize.begin(), lebSize.end(), std::back_inserter(v));
    std::copy(strippedKey.begin(), strippedKey.end(), std::back_inserter(v));
}

RangeScanContinueValuePayload::RangeScanContinueValuePayload(
        std::string_view payload)
    : payload(payload) {
    // Smallest payload would be 1byte leb128 and 1byte key
    if (payload.size() < 2) {
        throw std::invalid_argument(
                "RangeScanContinueValuePayload payload too small to decode");
    }
}

RangeScanContinueValuePayload::Record RangeScanContinueValuePayload::next() {
    if (payload.empty()) {
        return {};
    }

    // fixed meta + 2 bytes min for key + 2 bytes min for value
    Expects(payload.size() >=
            sizeof(cb::mcbp::response::RangeScanContinueMetaResponse) + 2 + 2);

    // First point to the fixed metadata
    const auto* meta = reinterpret_cast<
            const cb::mcbp::response::RangeScanContinueMetaResponse*>(
            payload.data());
    advance(sizeof(cb::mcbp::response::RangeScanContinueMetaResponse));

    auto key = nextView();
    Expects(!key.empty());
    auto value = nextView();
    Expects(!value.empty());
    return {key, value, *meta};
}

std::string_view RangeScanContinueValuePayload::nextView() {
    if (payload.empty()) {
        return {};
    }

    cb::const_byte_buffer data(reinterpret_cast<const uint8_t*>(payload.data()),
                               payload.size());

    auto length = cb::mcbp::unsigned_leb128<size_t>::decode(data);

    // Should not have zero
    Expects(length.first);

    // We have a view to return
    std::string_view view(reinterpret_cast<const char*>(length.second.data()),
                          length.first);

    // Now advance the payload for next
    payload = std::string_view{
            reinterpret_cast<const char*>(length.second.data()) + length.first,
            length.second.size() - length.first};

    return view;
}

void RangeScanContinueValuePayload::advance(size_t n) {
    Expects(payload.size() > n);
    payload = std::string_view{payload.data() + n, payload.size() - n};
}

// encodes the payload which is
// [fixed metadata][leb128 len][key][leb128 len][value]
void RangeScanContinueValuePayload::encode(std::vector<uint8_t>& v,
                                           const item_info& item) {
    // No need to prefix every key, repeating/redundant
    auto key = item.key.makeDocKeyWithoutCollectionID();

    // First the fixed metadata
    cb::mcbp::response::RangeScanContinueMetaResponse extras{
            item.flags,
            uint32_t(item.exptime),
            item.seqno,
            item.cas,
            item.datatype};
    auto eBuffer = extras.getBuffer();
    std::copy(eBuffer.begin(), eBuffer.end(), std::back_inserter(v));

    // Second copy key (leb128 prefix)
    cb::mcbp::unsigned_leb128<size_t> l2(key.size());
    std::copy(l2.begin(), l2.end(), std::back_inserter(v));
    std::copy(key.begin(), key.end(), std::back_inserter(v));

    // Final copy value (leb128 prefix)
    cb::mcbp::unsigned_leb128<size_t> l3(item.value[0].iov_len);
    std::copy(l3.begin(), l3.end(), std::back_inserter(v));

    cb::const_byte_buffer value{
            reinterpret_cast<const uint8_t*>(item.value[0].iov_base),
            item.value[0].iov_len};

    std::copy(value.begin(), value.end(), std::back_inserter(v));
}

} // namespace cb::mcbp::response