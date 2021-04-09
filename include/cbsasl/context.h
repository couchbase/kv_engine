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

#include <string>

namespace cb::sasl {

/**
 * An abstract context class to allow a common base class for the
 * client and server context.
 */
class Context {
public:
    virtual ~Context() = default;

    /**
     * Get the UUID used for errors by this connection. If none
     * is created a new one is generated.
     */
    std::string getUuid();

    /**
     * Do this context contain a UUID?
     */
    bool containsUuid() const {
        return !uuid.empty();
    }

protected:
    std::string uuid;
};

} // namespace cb::sasl