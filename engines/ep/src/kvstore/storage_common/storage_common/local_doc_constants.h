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

namespace LocalDocKey {
static constexpr std::string_view vbstate = "_local/vbstate";
static constexpr std::string_view manifest = "_local/collections/manifest";
static constexpr std::string_view openCollections = "_local/collections/open";
static constexpr std::string_view openScopes = "_local/scope/open";
static constexpr std::string_view droppedCollections =
        "_local/collections/dropped";
} // namespace LocalDocKey