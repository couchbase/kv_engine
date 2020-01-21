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
#pragma once

#include <mcbp/protocol/request.h>
#include <memcached/durability_spec.h>
#include <cstdint>
#include <vector>

/**
 * Base class for all FrameInfo objects
 */
class FrameInfo {
public:
    virtual ~FrameInfo();
    /**
     * Encode this FrameInfo object into the on the wire specification
     * for this FrameInfo object.
     */
    virtual std::vector<uint8_t> encode() const = 0;

protected:
    /// Encode a FrameInfoId and a blob according to the encoding
    /// rules in the binary protocol spec.
    std::vector<uint8_t> encode(cb::mcbp::request::FrameInfoId id,
                                cb::const_byte_buffer data) const;
};

class BarrierFrameInfo : public FrameInfo {
public:
    ~BarrierFrameInfo() override;
    std::vector<uint8_t> encode() const override;
};

class DurabilityFrameInfo : public FrameInfo {
public:
    explicit DurabilityFrameInfo(cb::durability::Level level,
                                 cb::durability::Timeout timeout = {})
        : level(level), timeout(timeout) {
    }
    ~DurabilityFrameInfo() override;
    std::vector<uint8_t> encode() const override;

protected:
    const cb::durability::Level level;
    const cb::durability::Timeout timeout;
};

class DcpStreamIdFrameInfo : public FrameInfo {
public:
    explicit DcpStreamIdFrameInfo(uint16_t id) : id(id){};
    ~DcpStreamIdFrameInfo() override;
    std::vector<uint8_t> encode() const override;

protected:
    const uint16_t id;
};

class OpenTracingContextFrameInfo : public FrameInfo {
public:
    explicit OpenTracingContextFrameInfo(std::string ctx)
        : ctx(std::move(ctx)) {
    }
    ~OpenTracingContextFrameInfo() override;
    std::vector<uint8_t> encode() const override;

protected:
    const std::string ctx;
};
