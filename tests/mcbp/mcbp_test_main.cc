/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2018-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */

#include <daemon/settings.h>
#include <folly/portability/GTest.h>
#include <logger/logger.h>

int main(int argc, char** argv) {
    cb::logger::createConsoleLogger();
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
