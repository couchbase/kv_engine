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
 * Sub-document API support.
 */

#pragma once

class Cookie;

/* Subdocument executor functions. */
void subdoc_get_executor(Cookie& cookie);
void subdoc_exists_executor(Cookie& cookie);
void subdoc_dict_add_executor(Cookie& cookie);
void subdoc_dict_upsert_executor(Cookie& cookie);
void subdoc_delete_executor(Cookie& cookie);
void subdoc_replace_executor(Cookie& cookie);
void subdoc_array_push_last_executor(Cookie& cookie);
void subdoc_array_push_first_executor(Cookie& cookie);
void subdoc_array_insert_executor(Cookie& cookie);
void subdoc_array_add_unique_executor(Cookie& cookie);
void subdoc_counter_executor(Cookie& cookie);
void subdoc_get_count_executor(Cookie& cookie);
void subdoc_replace_body_with_xattr_executor(Cookie& cookie);
void subdoc_multi_lookup_executor(Cookie& cookie);
void subdoc_multi_mutation_executor(Cookie& cookie);
