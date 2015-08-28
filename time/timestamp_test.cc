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
#include <memcached/isotime.h>
#include <string.h>
#include <chrono>
#include <iomanip>
#include <iostream>
#include <sstream>
#include <stdlib.h>

static char env[80];

int main(void)
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
