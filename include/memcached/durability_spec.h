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

#include "config.h"

#include <platform/sized_buffer.h>
#include <cstdint>
#include <stdexcept>
#include <string>

#ifndef WIN32
#include <arpa/inet.h>
#endif

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
 * The requirements specification for an operation.
 *
 * If the timeout is set to 0, use the system default value (the
 * memcached core populates the value to all of the engines so they'll
 * never see 0)
 */
class Requirements {
public:
    Requirements() = default;
    Requirements(const Requirements&) = default;

    constexpr Requirements(Level level_, uint16_t timeout_)
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
    explicit Requirements(cb::const_byte_buffer buffer) {
        if (buffer.size() != 1 && buffer.size() != 3) {
            throw std::invalid_argument(
                    "Requirements(): Invalid sized buffer provided: " +
                    std::to_string(buffer.size()));
        }
        level = Level(buffer.front());
        if (buffer.size() == 3) {
            timeout = ntohs(
                    *reinterpret_cast<const uint16_t*>(buffer.data() + 1));
        }
        if (!isValid()) {
            throw std::runtime_error(
                    R"(Requirements(): Content represents an invalid requirement specification)");
        }
    }

    Level getLevel() const {
        return level;
    }
    void setLevel(Level level) {
        Requirements::level = level;
    }
    uint16_t getTimeout() const {
        return timeout;
    }
    void setTimeout(uint16_t timeout) {
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

protected:
    Level level{Level::Majority};
    /// The timeout in ms
    uint16_t timeout{0};
};

static constexpr Requirements NoRequirements = {Level::None, 0};

std::string to_string(Requirements r);

} // namespace durability
} // namespace cb
