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
 * Publicly visible symbols for crash_engine.
 *
 * The crash engine is used for unit testing to verify that breakpad works
 * and that we get correct callstacks from libraries in shared objects.
 */
#pragma once
#include <memcached/engine.h>
#include <memcached/visibility.h>

MEMCACHED_PUBLIC_API
unique_engine_ptr create_crash_engine_instance();
