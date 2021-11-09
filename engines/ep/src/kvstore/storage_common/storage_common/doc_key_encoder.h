/*
 *   Copyright 2021-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */

#pragma once

#include <mcbp/protocol/unsigned_leb128.h>

#include <string>

std::string encodeDocKey(std::string_view key,
                         std::string_view collection,
                         bool prepare);
