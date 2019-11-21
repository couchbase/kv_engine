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

using cb::mcbp::request::FrameInfoId;

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
    std::vector<uint8_t> ret(1);
    ret[0] = uint8_t(FrameInfoId::DcpStreamId) << 0x04U;
    ret[0] |= sizeof(id);
    auto value = htons(id);
    const auto* ptr = reinterpret_cast<const char*>(&value);
    ret.insert(ret.end(), ptr, ptr + sizeof(value));

    return ret;
}

std::vector<uint8_t> OpenTracingContextFrameInfo::encode() const {
    std::vector<uint8_t> ret(1);
    ret[0] = uint8_t(FrameInfoId::OpenTracingContext) << 0x04U;
    if (ctx.size() < 0x0fU) {
        // We may fit in a single byte
        ret[0] |= uint8_t(ctx.size());
    } else {
        // we need an extra length byte to set the length
        ret[0] |= 0x0fU;
        ret.push_back(gsl::narrow<uint8_t>(ctx.size()));
    }
    ret.insert(ret.end(), ctx.cbegin(), ctx.cend());

    return ret;
}

std::vector<uint8_t> ImpersonateUserFrameInfo::encode() const {
    return FrameInfo::encode(
            FrameInfoId::Impersonate,
            {reinterpret_cast<const uint8_t*>(user.data()), user.size()});
}
