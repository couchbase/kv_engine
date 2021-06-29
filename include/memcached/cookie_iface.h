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

/**
 * The CookieIface is an abstract class representing a single command
 * when used from the frontend calling down into the underlying engine
 * (there is currently an exception to this, and that is that DCP connections
 * _currently_ use the Cookie to represent the connection)
 */
class CookieIface : public cb::tracing::Traceable {
public:
    /// Is the current cookie blocked?
    virtual bool isEwouldblock() const = 0;

    /// Set the ewouldblock status for the cookie
    virtual void setEwouldblock(bool ewouldblock) = 0;

    // The source code was initially written in C which didn't have the
    // concept of shared pointers so the current code use a manual
    // reference counting. If the engine wants to keep a reference to the
    // cookie it must bump the reference count to avoid the core to reuse
    // the cookie leaving the engine with a dangling pointer.

    /// Get the current reference count
    virtual uint8_t getRefcount() = 0;
    /// Add a reference to the cookie
    virtual void incrementRefcount() = 0;
    /// Release a reference to the cookie
    virtual void decrementRefcount() = 0;

    // The underlying engine may store information bound to the given cookie
    // in an opaque pointer. The framework will _NOT_ take ownership of the
    // data, and the engine must deal with all allocation/deallocation of the
    // memory to avoid any leaks.

    /// Get the value stored for the cookie (or nullptr if nothing was stored).
    /// The function returns the same value until it is being set to another
    /// value by callin setEngineStorage
    virtual void* getEngineStorage() const = 0;
    /// Set the engine pointer to the provided value
    virtual void setEngineStorage(void* value) = 0;

    /**
     * Inflate the value (if deflated); caching the inflated value inside the
     * cookie.
     *
     * @param header The packet header
     * @return true if success, false if an error occurs (the error context
     *         contains the reason why)
     */
    virtual bool inflateInputPayload(const cb::mcbp::Header& header) = 0;

    /// Get the payload from the command.
    virtual std::string_view getInflatedInputPayload() const = 0;
};
