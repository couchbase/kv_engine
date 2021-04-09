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
#include <memcached/isotime.h>
#include <string.h>
#include <chrono>
#include <iomanip>
#include <iostream>
#include <sstream>
#include <stdlib.h>

static char env[80];

int main()
{
    time_t now = 1426239360;
    std::string expected("2015-03-13T02:36:00.000000-07:00");
    std::string timezone("TZ=America/Los_Angeles");

#ifdef _MSC_VER
    timezone.assign("TZ=PST8PDT");
#endif

    strcpy(env, timezone.c_str());
    putenv(env);

    tzset();

    std::string timestamp = ISOTime::generatetimestamp(now, 0);
    if (timestamp != expected) {
        std::cerr << "Comparison failed" << std::endl
                  << "  Expected [" << expected << "]" << std::endl
                  << "  got      [" << timestamp << "]" << std::endl;
        return EXIT_FAILURE;
    }

    return EXIT_SUCCESS;
}
