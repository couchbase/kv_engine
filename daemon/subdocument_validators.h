/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2015 Couchbase, Inc
 *
 *   Licensed under the Apache License, Version 2.0 (the "License");
 *   you may not use this file except in compliance with the License.
 *   You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 *   Unless required by applicable law or agreed to in writing, software
 *   distributed under the License is distributed on an "AS IS" BASIS,
 *   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *   See the License for the specific language governing permissions and
 *   limitations under the License.
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

/* Possible valid extras lengths for single-path commands. */

// Extras could be pathlen + path flags ...
const size_t SUBDOC_BASIC_EXTRAS_LEN = sizeof(uint16_t) + sizeof(uint8_t);
// ... or pathlen + path flags + optional doc_flags:
const size_t SUBDOC_DOC_FLAG_EXTRAS_LEN =
        SUBDOC_BASIC_EXTRAS_LEN + sizeof(uint8_t);
// ... or pathlen + path flags + optional expiry (mutations only):
const size_t SUBDOC_EXPIRY_EXTRAS_LEN = SUBDOC_BASIC_EXTRAS_LEN + sizeof(uint32_t);
// ... or it may have the additional expiry and doc_flags:
const size_t SUBDOC_ALL_EXTRAS_LEN = SUBDOC_EXPIRY_EXTRAS_LEN + sizeof(uint8_t);

/* Possible extras lengths for multi-path commands. */

// Extras could just be (optional) doc flags ...
const size_t SUBDOC_MULTI_DOC_FLAG_EXTRAS_LEN = sizeof(uint8_t);
// ... or just (optional) expiry (mutations only):
const size_t SUBDOC_MULTI_EXPIRY_EXTRAS_LEN = sizeof(uint32_t);
// ... or expiry and doc flags:
const size_t SUBDOC_MULTI_ALL_EXTRAS_LEN =
        SUBDOC_MULTI_EXPIRY_EXTRAS_LEN + SUBDOC_MULTI_DOC_FLAG_EXTRAS_LEN;

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
