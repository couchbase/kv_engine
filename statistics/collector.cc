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

#include "statistics/collector.h"

#include "statistics/labelled_collector.h"
#include <hdrhistogram/hdrhistogram.h>
#include <memcached/dockey.h>

#include <string_view>

using namespace std::string_view_literals;
BucketStatCollector StatCollector::forBucket(std::string_view bucket) const {
    return {*this, bucket};
}

const cb::stats::StatDef& StatCollector::lookup(cb::stats::Key key) {
    Expects(size_t(key) < size_t(cb::stats::Key::enum_max));
    return cb::stats::statDefinitions[size_t(key)];
}
