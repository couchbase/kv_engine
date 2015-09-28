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
#include "session_cas.h"
#include <stdexcept>

SessionCas session_cas;

ENGINE_ERROR_CODE SessionCas::cas(uint64_t newValue, uint64_t casval,
                                  uint64_t& currentValue) {
    ENGINE_ERROR_CODE ret = ENGINE_SUCCESS;
    {
        std::lock_guard<std::mutex> lock(mutex);

        if (session_cas.counter > 0) {
            ret = ENGINE_EBUSY;
        } else {
            if (casval == 0 || casval == value) {
                value = newValue;
            } else {
                ret = ENGINE_KEY_EEXISTS;
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
