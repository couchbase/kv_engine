/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2018 Couchbase, Inc.
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

#include <boost/optional/optional.hpp>
#include <platform/sized_buffer.h>
#include <array>
#include <gsl/gsl>
#include <type_traits>

namespace cb {
namespace mcbp {

/**
 * Helper code for encode and decode of LEB128 values.
 * - mcbp encodes collection-ID as an unsigned LEB128
 * - see https://en.wikipedia.org/wiki/LEB128
 */

/**
 * decode_unsigned_leb128 returns the decoded uint<T> and the index of the last
 * byte of the leb128 within buf.
 * @param buf buffer containing a leb128 encoded value (of size T)
 * @returns std::pair first is the decoded value and second a buffer for the
 *          remaining data (size will be 0 for no more data)
 */
template <class T>
typename std::enable_if<std::is_unsigned<T>::value,
                        std::pair<T, cb::const_byte_buffer>>::type
decode_unsigned_leb128(cb::const_byte_buffer buf) {
    T rv = buf[0] & 0x7full;
    size_t end = 0;
    if ((buf[0] & 0x80) == 0x80ull) {
        T shift = 7;
        // shift in the remaining data
        for (end = 1; end < buf.size(); end++) {
            rv |= (buf[end] & 0x7full) << shift;
            if ((buf[end] & 0x80ull) == 0) {
                break; // no more
            }
            shift += 7;
        }

        // We should of stopped for a stop byte, not the end of the buffer
        if (end == buf.size()) {
            throw std::invalid_argument("decode_unsigned_leb128: no stop byte");
        }
    }
    // Return the decoded value and a buffer for any remaining data
    return {rv,
            cb::const_byte_buffer{buf.data() + end + 1,
                                  buf.size() - (end + 1)}};
}

/**
 * @return a buffer to the data after the leb128 prefix
 */
template <class T>
typename std::enable_if<std::is_unsigned<T>::value, cb::const_byte_buffer>::type
skip_unsigned_leb128(cb::const_byte_buffer buf) {
    return decode_unsigned_leb128<T>(buf).second;
}

/// @return the index of the stop byte within buf
static inline boost::optional<size_t> unsigned_leb128_get_stop_byte_index(
        cb::const_byte_buffer buf) {
    // If buf does not contain a stop-byte, invalid
    size_t stopByte = 0;
    for (auto c : buf) {
        if ((c & 0x80ull) == 0) {
            return stopByte;
        }
        stopByte++;
    }
    return {};
}

// Empty, non specialised version of the decoder class
template <class T, class Enable = void>
class unsigned_leb128 {};

/**
 * For encoding a unsigned T leb128, class constructs from a T value and
 * provides a const_byte_buffer for access to the encoded
 */
template <class T>
class unsigned_leb128<
        T,
        typename std::enable_if<std::is_unsigned<T>::value>::type> {
public:
    unsigned_leb128(T in) {
        while (in > 0) {
            auto byte = gsl::narrow_cast<uint8_t>(in & 0x7full);
            in >>= 7;

            // In has more data?
            if (in > 0) {
                byte |= 0x80;
                encodedData[encodedSize - 1] = byte;
                // Increase the size
                encodedSize++;
            } else {
                encodedData[encodedSize - 1] = byte;
            }
        }
    }

    cb::const_byte_buffer get() const {
        return {encodedData.data(), encodedSize};
    }

    const uint8_t* begin() const {
        return encodedData.data();
    }

    const uint8_t* end() const {
        return encodedData.data() + encodedSize;
    }

    const uint8_t* data() const {
        return encodedData.data();
    }

    size_t size() const {
        return encodedSize;
    }

    static size_t getMaxSize() {
        return maxSize;
    }

private:
    // Larger T may need a larger array
    static_assert(sizeof(T) <= 8, "Class is only valid for uint 8/16/64");

    // value is large enough to store ~0 as leb128
    static constexpr size_t maxSize = sizeof(T) + (((sizeof(T) + 1) / 8) + 1);
    std::array<uint8_t, maxSize> encodedData{};
    uint8_t encodedSize{1};
};

} // namespace mcbp
} // namespace cb