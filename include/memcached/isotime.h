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

#ifdef mcd_time_EXPORTS

#if defined (__SUNPRO_C) && (__SUNPRO_C >= 0x550)
#define ISOTIME_VISIBILITY __global
#elif defined __GNUC__
#define ISOTIME_VISIBILITY __attribute__ ((visibility("default")))
#elif defined(_MSC_VER)
#define ISOTIME_VISIBILITY __declspec(dllexport)
#else
/* unknown compiler */
#define ISOTIME_VISIBILITY
#endif

#else

#if defined(_MSC_VER)
#define ISOTIME_VISIBILITY __declspec(dllimport)
#else
#define ISOTIME_VISIBILITY
#endif

#endif

#include <memcached/visibility.h>
#include <cstdint>
#include <string>
#include <time.h>

#pragma once

class ISOTIME_VISIBILITY ISOTime {
public:
    static std::string generatetimestamp(void);
    static std::string generatetimestamp(time_t now_t, uint32_t frac_of_second);
};
