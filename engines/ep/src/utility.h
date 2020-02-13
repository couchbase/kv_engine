/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2015 Couchbase, Inc
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

/*
 * High level, generic utility functions and macros for wide use across
 * ep-engine.
 *
 * Note: Please keep this file as tight as possible - it is included by *many*
 *       other files and hence we want to keep the preprocessor/parse overhead
 *       as low as possible.
 *
 *       As a general rule don't add anything here unless at least 50% of
 *       the source files in ep_engine need it.
 */

#pragma once

#include <platform/dynamic.h>

#ifndef DISALLOW_COPY_AND_ASSIGN
// A macro to disallow the copy constructor and operator= functions
#define DISALLOW_COPY_AND_ASSIGN(TypeName)      \
    TypeName(const TypeName&) = delete;         \
    void operator=(const TypeName&) = delete
#endif

#ifndef DISALLOW_ASSIGN
#define DISALLOW_ASSIGN(TypeName)               \
    void operator=(const TypeName&) = delete
#endif
