/* -*- Mode: C; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2019 Couchbase, Inc
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
 * Publicly visible symbols for nobucket.so
 *
 * The "nobucket" is a bucket that just return "cb::engine_errc::no_bucket".
 * This bucket may be set as the "default" bucket for connections to avoid
 * having to check if a bucket is selected or not.
 */
#pragma once

#include <memcached/engine.h>

unique_engine_ptr create_no_bucket_instance();
