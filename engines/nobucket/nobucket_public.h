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
 * Publicly visible symbols for nobucket.so
 *
 * The "nobucket" is a bucket that just return "cb::engine_errc::no_bucket".
 * This bucket may be set as the "default" bucket for connections to avoid
 * having to check if a bucket is selected or not.
 */
#pragma once

#include <memcached/engine.h>

unique_engine_ptr create_no_bucket_instance();
