/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2017-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
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
