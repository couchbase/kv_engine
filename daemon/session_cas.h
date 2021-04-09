/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2015-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */
#pragma once

#include <memcached/engine_error.h>
#include <memcached/types.h>
#include <cstdint>
#include <mutex>

/**
 * Structure to save ns_server's session cas token.
 */
class SessionCas {
public:
    SessionCas()
        : value(0xdeadbeef),
          counter(0) {
        // empty
    }

    /**
     * Set the the current session cas value.
     *
     * @param newValue the new session cas value
     * @param casval the current cas value (0 is cas wildcard and overrides
     *               any value)
     * @param currentValue the current value (the new value if succeeded, the
     *                     previous value if failure)
     * @return cb::engine_errc::success upon success,
     *         ENGINE_BUSY if an operation is currently being performed
     *                     so the cas value can't be modified
     *         cb::engine_errc::key_already_exists if the provided cas value is
     * incorrect
     */
    cb::engine_errc cas(uint64_t newValue,
                        uint64_t casval,
                        uint64_t& currentValue);

    /**
     * Get the current CAS value
     */
    uint64_t getCasValue();

    /**
     * Increment the session counter if the cas matches
     *
     * @param cas the cas value for the operation
     * @return true if the session counter was incremented, false otherwise
     */
    bool increment_session_counter(const uint64_t cas);

    /**
     * Decrement the session counter (note this is _ONLY_ legal if you
     * ran a successful increment of the session counter!!)
     */
    void decrement_session_counter();

private:
    /**
     * The current CAS value. Do <b>not</b> modify this directly, but
     * use <code>cas</code> to update (it may not be modified unless
     * counter == 0)
     */
    uint64_t value;

    /**
     * Whenever we need to perform a potentially long-lived operation
     * protected by the session cas, we bump this counter and no one may
     * change the CAS value until we're done (used to protect ourself from
     * race conditions)
     */
    uint64_t counter;

    /**
     * All members in the class is protected with this mutex
     */
    std::mutex mutex;
};

extern SessionCas session_cas;
