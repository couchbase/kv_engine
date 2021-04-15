/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2018-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */

#pragma once

#include <mcbp/protocol/request.h>

#include <stdint.h>
#include <iosfwd>

namespace cb::mcbp {

/**
 * DcpStreamId - allows a client to chose a value to associate with a stream
 * or if no value is chosen (normal DCP) 0 is reserved for meaning off.
 */
class DcpStreamId {
public:
    DcpStreamId() = default;

    explicit DcpStreamId(uint16_t value) : id(value){};

    uint16_t to_network() const {
        return htons(id);
    }

    std::string to_string() const {
        if (id != 0) {
            return "sid:" + std::to_string(id);
        }
        return "sid:none";
    }

    explicit operator bool() const {
        return id != 0;
    }

    bool operator<(const DcpStreamId& other) const {
        return (id < other.id);
    }

    bool operator<=(const DcpStreamId& other) const {
        return (id <= other.id);
    }

    bool operator>(const DcpStreamId& other) const {
        return (id > other.id);
    }

    bool operator>=(const DcpStreamId& other) const {
        return (id >= other.id);
    }

    bool operator==(const DcpStreamId& other) const {
        return (id == other.id);
    }

    bool operator!=(const DcpStreamId& other) const {
        return !(*this == other);
    }

protected:
    uint16_t id{0};
};

std::ostream& operator<<(std::ostream& os, const cb::mcbp::DcpStreamId& id);

struct DcpStreamIdFrameInfo {
    explicit DcpStreamIdFrameInfo(DcpStreamId sid) {
        *reinterpret_cast<uint16_t*>(this->sid) = sid.to_network();
    }

    cb::const_byte_buffer getBuf() const {
        return cb::const_byte_buffer{reinterpret_cast<const uint8_t*>(this),
                                     sizeof(tag) + sizeof(sid)};
    }

    std::string_view getBuffer() const {
        auto buffer = getBuf();
        return {reinterpret_cast<const char*>(buffer.data()), buffer.size()};
    }

private:
    // FrameID:2, len:2
    static const uint8_t frameId = uint8_t(request::FrameInfoId::DcpStreamId);
    uint8_t tag{(frameId << 4) | 2};
    uint8_t sid[2]{};
};

static_assert(sizeof(DcpStreamIdFrameInfo) == 3,
              "DcpStreamIdFrameInfo should be 3 bytes");
} // namespace cb::mcbp
