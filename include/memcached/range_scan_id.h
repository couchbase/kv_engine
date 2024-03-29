/*
 *     Copyright 2022-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */

#pragma once

#include <boost/uuid/uuid.hpp>
#include <fmt/ostream.h>

namespace cb::rangescan {

// Each RangeScan has an id shared between server/client
using Id = boost::uuids::uuid;

} // namespace cb::rangescan

template <>
struct fmt::formatter<cb::rangescan::Id> : ostream_formatter {};
