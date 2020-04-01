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

#include <nlohmann/json_fwd.hpp>
#include <platform/sized_buffer.h>
#include <cstdint>
#include <stdexcept>
#include <string>

namespace cb {
namespace durability {

/**
 * The legal values for durability requirements.
 * For simplicity in the code the values is tightly coupled with
 * the on-the-wire representation
 */
enum class Level : uint8_t {
    /**
     * No durability required. Not expecting client requests to specify this
     * (equivalent to omitting the durability requirements from the request),
     * but included in the enum so ep_engine can use Level / Requirements
     * instances unconditionally and have None specify a "normal" op.
     */
    None = 0,
    Majority = 1,
    MajorityAndPersistOnMaster = 2,
    PersistToMajority = 3
};

/**
 * The timeout to use for this durability request. If the request cannot be
 * completed within the timeout (as measured by the Server once the request
 * has been accepted), then the request is aborted.
 *
 * Can also represent special values for:
 * - Engine specified default timeout (value 0)
 * - Infinite timeout (value 0xffff) - but note that clients are *not*
 *   permitted to encode this.
 */
class Timeout {
public:
    /// default ctor - creates with bucket default timeout.
    Timeout() = default;

    /**
     * ctor used to construct a timeout from a client-specified value on the
     * wire.
     * Permits a specific timeout in milliseconds (non-0, up to 0xfffe).
     * Does *not* allow specifying the special 'BucketDefault' timeout (0) -
     * just use the default ctor.
     * Does *not allow specifying the special 'Infinite' timeout (0xffff) -
     * that is reserved for internal usage.
     */
    constexpr Timeout(uint16_t val) {
        if (val == BucketDefaultVal) {
            throw std::invalid_argument(
                    "Timeout(): Cannot specify bucket default timeout");
        }
        if (val == InfinityVal) {
            throw std::invalid_argument(
                    "Timeout(): Cannot specify an infinite timeout");
        }
        value = val;
    }

    /// Factory method for an infinite timeout.
    static Timeout Infinity() {
        return Timeout(PrivateCtorTag(), InfinityVal);
    }

    uint16_t get() const {
        return value;
    }

    /// @returns true if this Timeout should use the Engine's default value.
    bool isDefault() const {
        return value == BucketDefaultVal;
    }

    /// @returns true if this Timeout is infinite.
    bool isInfinite() const {
        return value == InfinityVal;
    }

private:
    class PrivateCtorTag {};

    /// Private ctor which can construct the special values.
    Timeout(PrivateCtorTag, uint16_t val) : value(val) {
    }

    /// Special value to indicate the bucket's default timeout should be used.
    static constexpr uint16_t BucketDefaultVal{0};
    /// Special value to indicate an infinite timeout should be used.
    static constexpr uint16_t InfinityVal{0xffff};

    uint16_t value{BucketDefaultVal};
};

bool operator==(const Timeout& lhs, const Timeout& rhs);

std::string to_string(Timeout);

/**
 * The requirements specification for an operation.
 */
class Requirements {
public:
    Requirements() = default;
    Requirements(const Requirements&) = default;

    constexpr Requirements(Level level_, Timeout timeout_)
        : level(level_), timeout(timeout_) {
    }

    /**
     * Initialize a Requirement specification by parsing a byte buffer with
     * the following format:
     *
     *    1 byte representing the requrement level
     *    2 optional bytes representing the timeout
     *
     * This method is tightly coupled with the on-the-wire representation
     * in the frame extras. It is put here to avoid duplicating the code.
     *
     * @param buffer The byte buffer to parse
     * @throws std::invalid_arguments if the byte buffer size is of an illegal
     *                                size.
     * @throws std::runtime_error if the provided durability specification isn't
     *                            valid.
     */
    explicit Requirements(cb::const_byte_buffer buffer);

    Level getLevel() const {
        return level;
    }
    void setLevel(Level level) {
        Requirements::level = level;
    }
    Timeout getTimeout() const {
        return timeout;
    }
    void setTimeout(Timeout timeout) {
        Requirements::timeout = timeout;
    }

    /**
     * Does this represent a valid durability requirements request?
     * Note that Level::None is considered not valid; as it makes no sense
     * for clients to specify it.
     */
    bool isValid() const {
        // Timeout don't have any limitations.
        switch (level) {
        case Level::None:
            return false;
        case Level::Majority:
        case Level::MajorityAndPersistOnMaster:
        case Level::PersistToMajority:
            return true;
        }
        return false;
    }

    nlohmann::json to_json() const;

protected:
    Level level{Level::Majority};
    Timeout timeout;
};

bool operator==(const Requirements& lhs, const Requirements& rhs);

// @todo-durability: Might be able to remove this now we are using
// boost::optional for requirements in VBucket, and Item uses the queue_op
// to determine it's CommittedState. Check if any references remain.
// (Can also remove Level::None).
static constexpr Requirements NoRequirements = {Level::None, Timeout{}};

std::string to_string(Requirements r);
std::string to_string(Level l);
Level to_level(const std::string& s);

} // namespace durability
} // namespace cb
