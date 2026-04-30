/*
 *     Copyright 2026-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */

#pragma once

// There is a known issue with clang 15 and flatbuffers where it generates
// warnings about implicit int conversions. Ideally we should have listed
// the flatbuffers headers as SYSTEM include directories to avoid this, but
// that caused a failure in clang-analyze reporting a memory leak. Instead
// we can instruct the compiler to ignore the option instead while including
// the header.

#ifdef __clang_major__
#pragma clang diagnostic push
#pragma clang diagnostic ignored "-Wimplicit-int-conversion"
#endif

#include <flatbuffers/idl.h>

#ifdef __clang_major__
#pragma clang diagnostic pop
#endif
