/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2017 Couchbase, Inc.
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

#include <mcbp/protocol/datatype.h>
#include <mcbp/protocol/feature.h>

#include <atomic>

class DatatypeFilter {
public:
    /**
     * Translate the mcbp::Feature:: input into the corresponding mcbp datatype
     * bit.
     *
     * @param datatype mcbp::Feature::JSON|XATTR|SNAPPY
     * @throws if datatype is not a Feature datatype.
     */
    void enable(cb::mcbp::Feature datatype);

    /**
     * Enabled all datatypes
     */
    void enableAll();

    /**
     * Disable all datatypes
     */
    void disableAll();

    /**
     * Get the intersection of the input and enabled
     *
     * @param datatype the datatype to mask against the enabled types
     * @returns enabled_datatypes AND datatype
     */
    protocol_binary_datatype_t getIntersection(
            protocol_binary_datatype_t datatype) const;

    /**
     * Check if the bits in datatype are enabled in this instance
     *
     * @param datatype a set of datatype bits
     * @return true if all of the requested datatypes are enabled
     */
    bool isEnabled(protocol_binary_datatype_t datatype) const;

    /**
     * Helper method to test if the SNAPPY datatype is enabled
     */
    bool isSnappyEnabled() const {
        return isEnabled(PROTOCOL_BINARY_DATATYPE_SNAPPY);
    }

    /**
     * Helper method to test if the JSON datatype is enabled
     */
    bool isJsonEnabled() const {
        return isEnabled(PROTOCOL_BINARY_DATATYPE_JSON);
    }

    /**
     * Helper method to test if the XATTR datatype is enabled
     */
    bool isXattrEnabled() const {
        return isEnabled(PROTOCOL_BINARY_DATATYPE_XATTR);
    }

    protocol_binary_datatype_t getRaw() const {
        return protocol_binary_datatype_t(enabled.load());
    }

private:
    // One bit per enabled datatype
    std::atomic<uint8_t> enabled{0};
};
