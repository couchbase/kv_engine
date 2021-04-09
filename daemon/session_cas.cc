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
#include "session_cas.h"
#include <stdexcept>

SessionCas session_cas;

cb::engine_errc SessionCas::cas(uint64_t newValue,
                                uint64_t casval,
                                uint64_t& currentValue) {
    cb::engine_errc ret = cb::engine_errc::success;
    {
        std::lock_guard<std::mutex> lock(mutex);

        if (session_cas.counter > 0) {
            ret = cb::engine_errc::too_busy;
        } else {
            if (casval == 0 || casval == value) {
                value = newValue;
            } else {
                ret = cb::engine_errc::key_already_exists;
            }
        }
        currentValue = value;
    }

    return ret;
}

uint64_t SessionCas::getCasValue() {
    std::lock_guard<std::mutex> lock(mutex);
    return value;
}

void SessionCas::decrement_session_counter()  {
    std::lock_guard<std::mutex> lock(mutex);
    if (counter == 0) {
        throw std::logic_error("session counter can't be 0");
    }
    --counter;
}

bool SessionCas::increment_session_counter(const uint64_t cas)  {
    std::lock_guard<std::mutex> lock(mutex);
    bool ret = true;
    if (cas != 0) {
        if (value != cas) {
            ret = false;
        } else {
            counter++;
        }
    } else {
        counter++;
    }
    return ret;
}
