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

#include <cstdint>
#include <string>
#include <string_view>
#include <utility>

/**
 * Decodes the local doc if it needs decoding (determined by the key)
 * @param key of the local document (needed to select the correct decoder)
 * @param value string_view of the decompressed local document value
 * @return pair of bool status and decoded doc
 */
std::pair<bool, std::string> maybe_decode_local_doc(std::string_view key,
                                                    std::string_view value);

/**
 * Decodes the doc if it needs decoding (determined by the key) Only system
 * events would be decoded.
 * @param key of the document
 * @param value string_view of the decompressed document value
 * @param deleted true if the doc is deleted
 * @param datatype the datatype of the doc (after decompression)
 * @return pair of bool status and decoded doc
 */
std::pair<bool, std::string> maybe_decode_doc(std::string_view key,
                                              std::string_view value,
                                              bool deleted,
                                              uint8_t datatype);
