/*
 *     Copyright 2019-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
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

class ImpersonateUserFrameInfo : public FrameInfo {
public:
    explicit ImpersonateUserFrameInfo(std::string user)
        : user(std::move(user)) {
    }
    ~ImpersonateUserFrameInfo() override;
    std::vector<uint8_t> encode() const override;

protected:
    const std::string user;
};

class PreserveTtlFrameInfo : public FrameInfo {
public:
    ~PreserveTtlFrameInfo() override;
    std::vector<uint8_t> encode() const override;
};
