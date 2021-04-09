/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2010-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */

#pragma once

#include <platform/socket.h>
#include <string>

enum vbucket_state_t : int {
    vbucket_state_active = 1, /**< Actively servicing a vbucket. */
    vbucket_state_replica, /**< Servicing a vbucket as a replica only. */
    vbucket_state_pending, /**< Pending active. */
    vbucket_state_dead /**< Not in use, pending deletion. */
};

/**
 * Enumeration used by GET_ALL_VBSEQNOS for the client to specify which
 * vBucket(s) they are interested in. Allows both specific states, plus some
 * groups.
 */
enum class RequestedVBState : int {
    Alive = 0, /**< A value indicating the vBucket is not dead*/
    Active = vbucket_state_active,
    Replica = vbucket_state_replica,
    Pending = vbucket_state_pending,
    Dead = vbucket_state_dead,
};

#define is_valid_vbucket_state_t(state) \
    (state == vbucket_state_active || \
     state == vbucket_state_replica || \
     state == vbucket_state_pending || \
     state == vbucket_state_dead)

struct vbucket_failover_t {
    uint64_t uuid;
    uint64_t seqno;
};

/**
 * Vbid - a custom type class to control the use of vBucket ID's and their
 * output formatting, wrapping it with "vb:"
 */
class Vbid {
public:
    using id_type = uint16_t;

    Vbid() = default;

    explicit Vbid(id_type vbidParam) : vbid(vbidParam){};

    // Retrieve the vBucket ID in the form of uint16_t
    id_type get() const {
        return vbid;
    }

    // Retrieve the vBucket ID in a printable/loggable form
    std::string to_string() const {
        return "vb:" + std::to_string(vbid);
    }

    // Converting the byte order of Vbid's between host and network order
    // has been simplified with these functions
    Vbid ntoh() const {
        return Vbid(ntohs(vbid));
    }

    Vbid hton() const {
        return Vbid(htons(vbid));
    }

    bool operator<(const Vbid& other) const {
        return (vbid < other.get());
    }

    bool operator<=(const Vbid& other) const {
        return (vbid <= other.get());
    }

    bool operator>(const Vbid& other) const {
        return (vbid > other.get());
    }

    bool operator>=(const Vbid& other) const {
        return (vbid >= other.get());
    }

    bool operator==(const Vbid& other) const {
        return (vbid == other.get());
    }

    bool operator!=(const Vbid& other) const {
        return (vbid != other.get());
    }

    Vbid operator++() {
        return Vbid(++vbid);
    }

    Vbid operator++(int) {
        return Vbid(vbid++);
    }

protected:
    id_type vbid;
};

std::ostream& operator<<(std::ostream& os, const Vbid& d);
std::string to_string(const Vbid& vbucket);

namespace std {
template <>
struct hash<Vbid> {
public:
    size_t operator()(const Vbid& d) const {
        return static_cast<size_t>(d.get());
    }
};
} // namespace std
