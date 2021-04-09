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

#include <array>
#include <cstdint>
#include <mutex>
#include <string>
#include <time.h>

#pragma once

class ISOTime {
public:
    static std::string generatetimestamp();
    static std::string generatetimestamp(time_t now_t, uint32_t frac_of_second);


    typedef std::array<char, 33> ISO8601String;
    /**
     * Generate a timestamp from the current time and put it into the
     * specified array with a trailing '\0'
     *
     * @param destination Where to store the formatted string
     * @return the length of the timestamp
     */
    static int generatetimestamp(ISO8601String &destination);

    /**
     * Generate a timestamp from the specified time and put it into
     * the specified array with a trailing '\0'
     *
     * @param destination Where to store the formatted string
     * @param now the number of seconds since epoc
     * @param frac_of_second the fraction of a second (usec)
     * @return the lenght of the timestamp
     */
    static int generatetimestamp(ISO8601String &destination,
                                 time_t now, uint32_t frac_of_second);

private:
    /*
     * generatetimestamp makes use of mktime which is not thread-safe
     * and therefore we must protect the mktime call with a mutex.
     */
    static std::mutex mutex;
};
