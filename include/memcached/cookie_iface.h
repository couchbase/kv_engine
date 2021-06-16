/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2021-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */

#pragma once

#include <memcached/tracer.h>

namespace cb::mcbp {
class Header;
enum class Status : uint16_t;
} // namespace cb::mcbp

class CookieIface : public cb::tracing::Traceable {
public:
    virtual cb::mcbp::Status validate() = 0;
    virtual bool isEwouldblock() const = 0;
    virtual void setEwouldblock(bool ewouldblock) = 0;
    virtual uint8_t getRefcount() = 0;
    virtual void incrementRefcount() = 0;
    virtual void decrementRefcount() = 0;
    virtual void* getEngineStorage() const = 0;
    virtual void setEngineStorage(void* value) = 0;
    virtual bool inflateInputPayload(const cb::mcbp::Header& header) = 0;
    virtual std::string_view getInflatedInputPayload() const = 0;
};
