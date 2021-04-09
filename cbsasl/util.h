/*
 *     Copyright 2013-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */
#pragma once

#include <cstddef>

/* Compare a and b without revealing their content by short-circuiting */
int cbsasl_secure_compare(const char* a,
                          size_t alen,
                          const char* b,
                          size_t blen);
