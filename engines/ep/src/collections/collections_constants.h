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

#include <nlohmann/json.hpp>

static constexpr const nlohmann::json::value_t CollectionsType =
        nlohmann::json::value_t::array;
static constexpr nlohmann::json::value_t ScopeType =
        nlohmann::json::value_t::string;
static constexpr nlohmann::json::value_t UidType =
        nlohmann::json::value_t::string;
static constexpr nlohmann::json::value_t StreamIdType =
        nlohmann::json::value_t::number_unsigned;
static constexpr nlohmann::json::value_t ForceUpdateType =
        nlohmann::json::value_t::boolean;
