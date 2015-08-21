/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2015 Couchbase, Inc.
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
#pragma once

// This is a top-level header that includes all of the code needed in
// order to use libgreenstack. It includes all of the class definitions
// for all of the various request/response builders etc. If you only need
// a subset of the files feel free to just include what you need. This
// is just a convenience header.

#include <libgreenstack/Frame.h>
#include <libgreenstack/Request.h>
#include <libgreenstack/Response.h>
#include <libgreenstack/Opcodes.h>
#include <libgreenstack/Reader.h>
#include <libgreenstack/Writer.h>
#include <libgreenstack/Status.h>
#include <libgreenstack/core/Hello.h>
#include <libgreenstack/core/Keepalive.h>
#include <libgreenstack/core/SaslAuth.h>
#include <libgreenstack/memcached/AssumeRole.h>
#include <libgreenstack/memcached/CreateBucket.h>
#include <libgreenstack/memcached/DeleteBucket.h>
#include <libgreenstack/memcached/Get.h>
#include <libgreenstack/memcached/ListBuckets.h>
#include <libgreenstack/memcached/SelectBucket.h>
#include <libgreenstack/memcached/Mutation.h>
