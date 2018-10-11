/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2018 Couchbase, Inc
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

#include <memcached/mcd_util-visibility.h>
#include <platform/socket.h>

#include <stdint.h>
#include <ostream>

/**
 * DcpStreamId - allows a client to chose a value to associate with a stream
 * or if no value is chosen (normal DCP) 0 is reserved for meaning off.
 */
class MCD_UTIL_PUBLIC_API DcpStreamId {
public:
    DcpStreamId() = default;

    explicit DcpStreamId(uint16_t value) : id(value){};

    uint16_t hton() const {
        return htons(id);
    }

    std::string to_string() const {
        if (id != 0) {
            return "sid:" + std::to_string(id);
        }
        return "sid:none";
    }

    operator bool() const {
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

MCD_UTIL_PUBLIC_API
std::ostream& operator<<(std::ostream&, const DcpStreamId);