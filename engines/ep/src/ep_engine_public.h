/* -*- Mode: C; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2019-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */

/**
 * Publicly visible symbols for ep.so
 */
#pragma once
#include <memcached/engine.h>
#include <memcached/visibility.h>

MEMCACHED_PUBLIC_API
cb::engine_errc create_ep_engine_instance(GET_SERVER_API get_server_api,
                                          EngineIface** handle);

MEMCACHED_PUBLIC_API
void destroy_ep_engine();
