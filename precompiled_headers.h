/*
 *     Copyright 2021-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */

#pragma once

#include <gsl/gsl-lite.hpp>
#include <nlohmann/json.hpp>

// Used by most unit test files.
#include <folly/portability/GTest.h>

// Used throughout the codebase
#include <folly/SharedMutex.h>
#include <folly/Synchronized.h>

// MB-46844:
// Included by collections/vbucket_manifest.h, which in turn included
// by 50+ other files.
// Consider changing collections/vbucket_manifest.h to use pimpl for
// Manifest::map which would avoid the need to include F14Map.h.
#include <folly/container/F14Map.h>

#include <map>
#include <string>
