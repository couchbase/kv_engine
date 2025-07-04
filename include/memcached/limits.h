/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2019-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */
#pragma once

#include <platform/byte_literals.h>
#include <platform/cb_arena_malloc_client.h>

#include <cstddef>

namespace cb::limits {

/// The total number of buckets which may be created on the server, limited by
/// how many concurrent arenas are available
constexpr std::size_t TotalBuckets = cb::ArenaMallocMaxClients;

/// The total number of bytes which may be used from privileged (system)
/// users in a document
constexpr std::size_t PrivilegedBytes = 1_MiB;

/// The maximum length of a username
constexpr std::size_t MaxUsernameLength = 128;

/// We specify a finite number of times to retry; to prevent the event that
/// we are fighting with another client for the correct CAS value for an
/// arbitrary amount of time (and to defend against possible bugs in our
/// code ;)
constexpr std::size_t SubdocMaxAutoRetries = 100;

} // namespace cb::limits
