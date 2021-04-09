/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2019-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */
#pragma once

#include <memory>

/*
 * Forward declarations of VBucket and related types.
 *
 * Note: If you _just_ need to refer to the VBucket class only you can simply
 * use a forward declaration:
 *
 *     class VBucket;
 *
 * This file is only necessary if you need other related types, such as
 * VBucketPtr.
 */

class VBucket;

using VBucketPtr = std::shared_ptr<VBucket>;
