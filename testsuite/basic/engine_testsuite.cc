/*
 *     Copyright 2019-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */
#include "engine_testsuite.h"
#include <daemon/enginemap.h>
#include <logger/logger.h>
#include <platform/dirutils.h>
#include <platform/platform_socket.h>
#include <programs/engine_testapp/mock_engine.h>
#include <programs/engine_testapp/mock_server.h>

void EngineTestsuite::SetUpTestCase() {
    cb::logger::createConsoleLogger();
    cb::net::initialize();

    auto limit = cb::io::maximizeFileDescriptors(1024);
    if (limit < 1024) {
        std::cerr << "Error: The unit tests needs at least 1k file "
                     "descriptors"
                  << std::endl;
        exit(EXIT_FAILURE);
    }
    init_mock_server();
}

void EngineTestsuite::TearDownTestCase() {
    shutdown_all_engines();
}

std::unique_ptr<EngineIface> EngineTestsuite::createBucket(
        BucketType bucketType,
        const std::string& cfg) {
    auto handle = new_engine_instance(bucketType, &get_mock_server_api);
    if (!handle) {
        throw std::runtime_error("createBucket: failed to create bucket");
    }

    auto me = std::make_unique<MockEngine>(std::move(handle));
    const auto error =
            me->the_engine->initialize(cfg.empty() ? nullptr : cfg.c_str());
    if (error != cb::engine_errc::success) {
        me->the_engine->destroy(false /*force*/);
        throw cb::engine_error{cb::engine_errc(error),
                               "Failed to initialize instance"};
    }
    return me;
}
