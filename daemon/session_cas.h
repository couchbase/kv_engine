/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2015 Couchbase, Inc.
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

#include <cstdint>
#include <mutex>
#include <memcached/types.h>

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
     * @return ENGINE_SUCCESS upon success,
     *         ENGINE_BUSY if an operation is currently being performed
     *                     so the cas value can't be modified
     *         ENGINE_KEY_EEXISTS if the provided cas value is incorrect
     */
    ENGINE_ERROR_CODE cas(uint64_t newValue,
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
