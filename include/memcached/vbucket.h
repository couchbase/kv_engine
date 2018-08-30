/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2010 Couchbase, Inc
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
#include <platform/bitset.h>
#include <string>

typedef enum : int {
    vbucket_state_active = 1, /**< Actively servicing a vbucket. */
    vbucket_state_replica, /**< Servicing a vbucket as a replica only. */
    vbucket_state_pending, /**< Pending active. */
    vbucket_state_dead /**< Not in use, pending deletion. */
} vbucket_state_t;

#define is_valid_vbucket_state_t(state) \
    (state == vbucket_state_active || \
     state == vbucket_state_replica || \
     state == vbucket_state_pending || \
     state == vbucket_state_dead)

typedef struct {
    uint64_t uuid;
    uint64_t seqno;
} vbucket_failover_t;

struct PermittedVBStatesMap {
    size_t map(vbucket_state_t in) {
        return in - 1;
    }
};

using PermittedVBStates = cb::bitset<4, vbucket_state_t, PermittedVBStatesMap>;

/**
 * Vbid - a custom type class to control the use of vBucket ID's and their
 * output formatting, wrapping it with "vb:"
 */
class MCD_UTIL_PUBLIC_API Vbid {
public:
    Vbid() = default;

    // TODO: Not explicit to support conversion with previous usage
    Vbid(uint16_t vbidParam) : vbid(vbidParam){};

    // Retrieve the vBucket ID in the form of uint16_t
    const uint16_t get() const {
        return vbid;
    }

    // Retrieve the vBucket ID in a printable/loggable form
    std::string to_string() const {
        return "vb:" + std::to_string(vbid);
    }

    size_t hash() const {
        return std::hash<uint16_t>()(vbid);
    }

    bool operator<(const Vbid& other) {
        return (vbid < other.get());
    }

    bool operator<=(const Vbid& other) {
        return (vbid <= other.get());
    }

    bool operator>(const Vbid& other) {
        return (vbid > other.get());
    }

    bool operator>=(const Vbid& other) {
        return (vbid >= other.get());
    }

    bool operator==(const Vbid& other) {
        return (vbid == other.get());
    }

    Vbid operator++() {
        return Vbid(++vbid);
    }

    const Vbid operator++(int) {
        return Vbid(vbid++);
    }

    // TODO: Support previous usage of vBucket ID as a uint16_t, which is
    // planned to be phased out in stages due to size of change
    operator uint16_t() const {
        return vbid;
    }

protected:
    uint16_t vbid;
};

MCD_UTIL_PUBLIC_API
std::ostream& operator<<(std::ostream& os, const Vbid& d);

namespace std {
template <>
class hash<Vbid> {
public:
    size_t operator()(const Vbid& d) const {
        return d.hash();
    }
};
} // namespace std