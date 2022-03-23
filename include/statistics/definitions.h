/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2020-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */

#pragma once

#include "statdef.h"
#include "units.h"

#include <string>
#include <string_view>
#include <unordered_map>

namespace cb::stats {

// don't need labels for the enum
#define LABEL(...)
// don't need formatted cbstats keys for the enum, only the enum key is needed.
#define FMT(CBStatName)
#define STAT(statName, ...) statName,
#define CBSTAT(statName, ...) statName,
#define PSTAT(statName, ...) statName,
enum class Key {
#include "stats.def.h"

    enum_max
};
#undef PSTAT
#undef CBSTAT
#undef STAT
#undef FMT
#undef LABEL

using namespace units;

extern const std::array<StatDef, size_t(Key::enum_max)> statDefinitions;

} // namespace cb::stats
