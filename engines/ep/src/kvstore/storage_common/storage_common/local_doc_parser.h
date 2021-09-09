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

#include <string>
#include <string_view>
#include <utility>

/**
 * Decodes the local doc if it needs decoding (determined by the key)
 * @param id key
 * @param v value
 * @param decodedData [out]
 * @return pair of bool status and decoded doc
 */
std::pair<bool, std::string> maybe_decode_local_doc(std::string_view key,
                                                    std::string_view value);
