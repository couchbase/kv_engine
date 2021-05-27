/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2017-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */
#pragma once

#include <gsl/gsl-lite.hpp>
#include <memcached/engine_error.h>
#include <memcached/types.h>
#include <memcached/vbucket.h>

class Cookie;
/** Callback from the engine adding the response */
cb::engine_errc add_failover_log(std::vector<vbucket_failover_t> entries,
                                 Cookie& cookie);
