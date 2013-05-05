/* -*- Mode: C; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2010-2013 Couchbase, Inc.
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

#include <platform/visibility.h>

#ifdef  __cplusplus
extern "C" {
#endif

#define no_argument 0
#define required_argument 1
#define optional_argument 2

    struct option {
        const char *name;
        int has_arg;
        int *flag;
        int val;
    };

    PLATFORM_PUBLIC_API
    extern char *optarg;
    PLATFORM_PUBLIC_API
    extern int opterr;
    PLATFORM_PUBLIC_API
    extern int optind;
    PLATFORM_PUBLIC_API
    extern int optopt;

    PLATFORM_PUBLIC_API
    extern int getopt_long(int argc, char **argv, const char *optstring,
                           const struct option *longopts, int *longindex);

    PLATFORM_PUBLIC_API
    extern int getopt(int argc, char **argv, const char *optstring);

#ifdef  __cplusplus
}
#endif
