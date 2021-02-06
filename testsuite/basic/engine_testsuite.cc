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
#include "engine_testsuite.h"
#include <daemon/enginemap.h>
#include <logger/logger.h>
#include <platform/dirutils.h>
#include <platform/platform_socket.h>
#include <programs/engine_testapp/mock_engine.h>
#include <programs/engine_testapp/mock_server.h>

void EngineTestsuite::SetUpTestCase() {
    cb::logger::createConsoleLogger();
    cb_initialize_sockets();

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
