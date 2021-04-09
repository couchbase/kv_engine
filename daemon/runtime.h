/* -*- Mode: C; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
*     Copyright 2015-Present Couchbase, Inc.
*
*   Use of this software is governed by the Business Source License included
*   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
*   in that file, in accordance with the Business Source License, use of this
*   software will be governed by the Apache License, Version 2.0, included in
*   the file licenses/APL2.txt.
*/

/*
 * Due to the fact that the memcached daemon is written in C we need
 * this little wrapper to provide atomics functionality without having
 * to reinvent the wheel
 */
#pragma once

#include <openssl/ossl_typ.h>

#include <string>
#include <vector>
#include <memory>

class Hdr1sfMicroSecHistogram;

bool is_default_bucket_enabled();
void set_default_bucket_enabled(bool enabled);

extern std::vector<Hdr1sfMicroSecHistogram> scheduler_info;
