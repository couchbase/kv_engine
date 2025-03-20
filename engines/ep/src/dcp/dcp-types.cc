/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2016-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */

#include "dcp/dcp-types.h"

#include "folly/lang/Assume.h"

#include <string>

std::string to_string(MarkerVersion version) {
    switch (version) {
    case MarkerVersion::V1_0:
        return "V1_0";
    case MarkerVersion::V2_0:
        return "V2_0";
    case MarkerVersion::V2_2:
        return "V2_2";
    }
    folly::assume_unreachable();
}

std::string to_string(IncludeValue includeValue) {
    switch (includeValue) {
    case IncludeValue::Yes:
        return "IncludeValue::Yes";
    case IncludeValue::No:
        return "IncludeValue::No";
    case IncludeValue::NoWithUnderlyingDatatype:
        return "IncludeValue::NoWithUnderlyingDatatype";
    }
    folly::assume_unreachable();
}
