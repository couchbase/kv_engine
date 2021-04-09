/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2017-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */

#include "callbacks.h"

#include "item.h"

GetValue::GetValue()
    : id(-1), status(cb::engine_errc::no_such_key), partial(false) {
}
GetValue::GetValue(GetValue&& other) = default;
GetValue& GetValue::operator=(GetValue&& other) = default;

GetValue::GetValue(std::unique_ptr<Item> v,
                   cb::engine_errc s,
                   uint64_t i,
                   bool incomplete)
    : item(std::move(v)), id(i), status(s), partial(incomplete) {
}

GetValue::~GetValue() = default;
