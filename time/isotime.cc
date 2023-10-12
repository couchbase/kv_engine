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
#include <chrono>
#include <stdlib.h>
#include <cstring>

#include <memcached/isotime.h>
#include <platform/checked_snprintf.h>

std::mutex ISOTime::mutex;

int ISOTime::generatetimestamp(ISO8601String &destination,
                               time_t now, uint32_t frac_of_second)
{
    struct tm utc_time;
    struct tm local_time;
#ifdef WIN32
    gmtime_s(&utc_time, &now);
    localtime_s(&local_time, &now);
#else
    gmtime_r(&now, &utc_time);
    localtime_r(&now, &local_time);
#endif
    time_t utc;
    time_t local;

    {
        std::lock_guard<std::mutex> lock(mutex);
        utc = mktime(&utc_time);
        local = mktime(&local_time);
    }

    if (utc == time_t(-1)) {
        throw std::runtime_error(
                "ISOTime::generatetimestamp(): mktime(utc_time) failed");
    }

    if (local == time_t(-1)) {
        throw std::runtime_error(
                "ISOTime::generatetimestamp(): mktime(local_time) failed");
    }

    if (utc_time.tm_isdst != 0) {
        // UTC should not be adjusted to daylight savings
        utc -= 3600;
    }

    double total_seconds_diff = difftime(local, utc);
    double total_minutes_diff = total_seconds_diff / 60;
    auto hours = (int32_t)(total_minutes_diff / 60);
    int32_t minutes = (int32_t)(total_minutes_diff) % 60;

    int offset = checked_snprintf(destination.data(), destination.size(),
                                  "%04u-%02u-%02uT%02u:%02u:%02u.%06u",
                                  local_time.tm_year + 1900,
                                  local_time.tm_mon + 1,
                                  local_time.tm_mday,
                                  local_time.tm_hour,
                                  local_time.tm_min,
                                  local_time.tm_sec,
                                  frac_of_second);

    if (total_seconds_diff == 0.0) {
        strcat(destination.data(), "Z");
        ++offset;
    } else if (total_seconds_diff < 0.0) {
        offset += checked_snprintf(destination.data() + offset,
                                   destination.size() - offset,
                                   "-%02u:%02u", abs(hours), abs(minutes));
    } else {
        offset += checked_snprintf(destination.data() + offset,
                                   destination.size() - offset,
                                   "+%02u:%02u", hours, minutes);
    }
    return offset;
}

int ISOTime::generatetimestamp(ISO8601String &destination) {
    using namespace std::chrono;

    system_clock::time_point now = system_clock::now();
    system_clock::duration seconds_since_epoch = duration_cast<seconds>(now.time_since_epoch());
    time_t now_t(system_clock::to_time_t(system_clock::time_point(seconds_since_epoch)));
    microseconds frac_of_second (duration_cast<microseconds>(now.time_since_epoch() - seconds_since_epoch));

    return generatetimestamp(destination, now_t,
                             static_cast<uint32_t>(frac_of_second.count()));
}

std::string ISOTime::generatetimestamp(time_t now_t, uint32_t frac_of_second) {
    ISO8601String buffer;
    generatetimestamp(buffer, now_t, frac_of_second);
    return std::string(buffer.data());
}

std::string ISOTime::generatetimestamp() {
    ISO8601String buffer;
    generatetimestamp(buffer);
    return std::string(buffer.data());
}
