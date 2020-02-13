/*
 *     Copyright 2019 Couchbase, Inc.
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

#include "frameinfo.h"

#include <gsl.h>
#include <mcbp/protocol/request.h>

FrameInfo::~FrameInfo() = default;
BarrierFrameInfo::~BarrierFrameInfo() = default;
DurabilityFrameInfo::~DurabilityFrameInfo() = default;
DcpStreamIdFrameInfo::~DcpStreamIdFrameInfo() = default;
OpenTracingContextFrameInfo::~OpenTracingContextFrameInfo() = default;
ImpersonateUserFrameInfo::~ImpersonateUserFrameInfo() = default;
PreserveTtlFrameInfo::~PreserveTtlFrameInfo() = default;

using cb::mcbp::request::FrameInfoId;

std::vector<uint8_t> FrameInfo::encode(cb::mcbp::request::FrameInfoId id,
                                       cb::const_byte_buffer data) const {
    // From the spec:
    //
    // * 4 bits: *Object Identifier*. Encodes first 15 object IDs directly; with
    // the 16th value (15) used
    //   as an escape to support an additional 256 IDs by combining the value of
    //   the next byte:
    //   * `0..14`: Identifier for this element.
    //   * `15`: Escape: ID is 15 + value of next byte.
    //* 4 bits: *Object Length*. Encodes sizes 0..14 directly; value 15 is
    //   used to encode sizes above 14 by combining the value of a following
    //   byte:
    //   * `0..14`: Size in bytes of the element data.
    //   * `15`: Escape: Size is 15 + value of next byte (after any object ID
    //   escape bytes).
    //* N Bytes: *Object data*.

    if (uint8_t(id) > 0xf) {
        throw std::runtime_error(
                "FrameInfo::encode: Multibyte frame identifiers not supported");
    }

    std::vector<uint8_t> ret(1);
    ret[0] = uint8_t(id) << 0x04U;
    if (data.size() < 0x0fU) {
        // We may fit in a single byte
        ret[0] |= uint8_t(data.size());
    } else {
        // we need an extra length byte to set the length
        ret[0] |= 0x0fU;
        ret.push_back(gsl::narrow<uint8_t>(data.size() - 0x0fU));
    }
    ret.insert(ret.end(), data.cbegin(), data.cend());
    return ret;
}

std::vector<uint8_t> BarrierFrameInfo::encode() const {
    std::vector<uint8_t> ret;
    ret.push_back(uint8_t(FrameInfoId::Barrier) << 0x04U);
    return ret;
}

std::vector<uint8_t> DurabilityFrameInfo::encode() const {
    using cb::durability::Level;
    using cb::durability::Timeout;

    if (level == Level::None) {
        return {};
    }

    std::vector<uint8_t> ret(2);
    ret[0] = uint8_t(FrameInfoId::DurabilityRequirement) << 0x04U;
    ret[1] = (uint8_t(0x0f) & uint8_t(level));

    if (timeout.isDefault()) {
        // Level is the only byte
        ret[0] |= uint8_t(0x01);
    } else {
        // Level and timeout is present
        ret[0] |= uint8_t(0x03);
        auto value = htons(timeout.get());
        const auto* ptr = reinterpret_cast<const char*>(&value);
        ret.insert(ret.end(), ptr, ptr + sizeof(value));
    }

    return ret;
}

std::vector<uint8_t> DcpStreamIdFrameInfo::encode() const {
    auto value = htons(id);
    return FrameInfo::encode(
            FrameInfoId::DcpStreamId,
            {reinterpret_cast<const uint8_t*>(&value), sizeof(value)});
}

std::vector<uint8_t> OpenTracingContextFrameInfo::encode() const {
    return FrameInfo::encode(
            FrameInfoId::OpenTracingContext,
            {reinterpret_cast<const uint8_t*>(ctx.data()), ctx.size()});
}

std::vector<uint8_t> ImpersonateUserFrameInfo::encode() const {
    return FrameInfo::encode(
            FrameInfoId::Impersonate,
            {reinterpret_cast<const uint8_t*>(user.data()), user.size()});
}

std::vector<uint8_t> PreserveTtlFrameInfo::encode() const {
    std::vector<uint8_t> ret;
    ret.push_back(uint8_t(FrameInfoId::PreserveTtl) << 0x04U);
    return ret;
}
