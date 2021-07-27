/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2021-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */

#pragma once

#include <string_view>
#include <unordered_map>

/// Forward declaration for typedefs used in kvstore.h.

/// GetStatsMap contains the result of fetching statistic values in bulk from
/// kvstore. The set of statistics returned is an intersection of what was
/// requested and what is supported by the kvstore implementation. Map key is
/// the name of the statistic. Map value is the statistic value.
using GetStatsMap = std::unordered_map<std::string_view, size_t>;

/// Result of flushing a Deletion, passed to the PersistenceCallback.
enum class FlushStateDeletion { Delete, DocNotFound, Failed };

/// Result of flushing a Mutation, passed to the PersistenceCallback.
enum class FlushStateMutation { Insert, Update, Failed };
