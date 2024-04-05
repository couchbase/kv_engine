/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2015-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */

/**
 * Sub-document API validator functions.
 */

#pragma once

#include <cstddef>
#include <cstdint>

namespace cb::mcbp {
enum class Status : uint16_t;
} // namespace cb::mcbp
class Cookie;

/* Maximum sub-document path length */
const size_t SUBDOC_PATH_MAX_LENGTH = 1024;

// Maximum length for an xattr key
const size_t SUBDOC_MAX_XATTR_LENGTH = 16;

/* Subdocument validator functions. Returns 0 if valid, else -1. */
cb::mcbp::Status subdoc_get_validator(Cookie& cookie);
cb::mcbp::Status subdoc_exists_validator(Cookie& cookie);
cb::mcbp::Status subdoc_dict_add_validator(Cookie& cookie);
cb::mcbp::Status subdoc_dict_upsert_validator(Cookie& cookie);
cb::mcbp::Status subdoc_delete_validator(Cookie& cookie);
cb::mcbp::Status subdoc_replace_validator(Cookie& cookie);
cb::mcbp::Status subdoc_array_push_last_validator(Cookie& cookie);
cb::mcbp::Status subdoc_array_push_first_validator(Cookie& cookie);
cb::mcbp::Status subdoc_array_insert_validator(Cookie& cookie);
cb::mcbp::Status subdoc_array_add_unique_validator(Cookie& cookie);
cb::mcbp::Status subdoc_counter_validator(Cookie& cookie);
cb::mcbp::Status subdoc_get_count_validator(Cookie& cookie);
cb::mcbp::Status subdoc_multi_lookup_validator(Cookie& cookie);
cb::mcbp::Status subdoc_multi_mutation_validator(Cookie& cookie);
cb::mcbp::Status subdoc_replace_body_with_xattr_validator(Cookie& cookie);
