/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2020-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */

/**
 * Helpers for collections tests.
 */
#pragma once

#include "collections/manifest.h"
#include <utilities/test_manifest.h>

/// Create a Collections::Manifest from a CollectionsManifest.
inline Collections::Manifest makeManifest(const CollectionsManifest& cm) {
    return Collections::Manifest{std::string{cm}};
}
