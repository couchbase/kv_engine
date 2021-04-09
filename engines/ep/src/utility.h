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
