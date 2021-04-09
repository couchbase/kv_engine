/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2016-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */
#include <platform/cbassert.h>

int memcached_main(int argc, char** argv);

/**
 * This is the program entry point for memcached. We're building the
 * memcached source as a static archive so that we may link it into other
 * programs (like memcached_testapp) and run it inside the process. The
 * main motivation for doing so is to make the life a lot easier when
 * trying to debug a unit test case (so that you can set breakpoints in
 * the same IDE rather than having to add spin locks and attach another
 * debugger to the memcached process spawned by the test case etc)
 *
 * @param argc argument count
 * @param argv argument vector
 * @return the process exit number
 */
int main(int argc, char** argv) {
    setupWindowsDebugCRTAssertHandling();
    return memcached_main(argc, argv);
}
