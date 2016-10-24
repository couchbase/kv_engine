/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2015 Couchbase, Inc
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
#include "enginemap.h"
#include "utilities/engine_loader.h"

#include <map>
#include <string>
#include <string.h>
#include <sstream>
#include <iostream>
#include <platform/cb_malloc.h>
#include <platform/dirutils.h>

class Engine {
public:
    Engine(const std::string &mod, engine_reference* ref,
           EXTENSION_LOGGER_DESCRIPTOR *log) :
        module(mod),
        engine_ref(ref),
        logger(log)
    {
        // EMPTY
    }

    ~Engine() {
        unload_engine(engine_ref);
    }

    bool createInstance(GET_SERVER_API get_server_api, ENGINE_HANDLE **handle) {
        return create_engine_instance(engine_ref, get_server_api,
                                      logger, handle);
    }

    const std::string &getModule() const {
        return module;
    }

private:
    const std::string module;
    engine_reference* engine_ref;
    EXTENSION_LOGGER_DESCRIPTOR *logger;
};

Engine *createEngine(const std::string &so,
                     const std::string &function,
                     EXTENSION_LOGGER_DESCRIPTOR *logger)
{
    engine_reference* engine_ref = load_engine(so.c_str(), function.c_str(),
                                               NULL, logger);

    if (engine_ref == NULL) {
        std::stringstream ss;
        ss << "Failed to load engine \"" << so << "\" with symbol \""
           << function << "\"";
        throw ss.str();
    }

    return new Engine(so, engine_ref, logger);
}

std::map<BucketType, Engine *> map;

bool new_engine_instance(BucketType type,
                         GET_SERVER_API get_server_api,
                         ENGINE_HANDLE **handle,
                         EXTENSION_LOGGER_DESCRIPTOR *logger)
{
    auto iter = map.find(type);
    cb_assert(iter != map.end());

    auto ret = iter->second->createInstance(get_server_api, handle);
    if (ret) {
        log_engine_details(*handle, logger);
    }

    return ret;
}

bool initialize_engine_map(char **msg, EXTENSION_LOGGER_DESCRIPTOR *logger)
{
    try {
        map[BucketType::NoBucket] = createEngine("nobucket.so",
                                                 "create_no_bucket_instance",
                                                 logger);
        map[BucketType::Memcached] = createEngine("default_engine.so",
                                                  "create_instance",
                                                  logger);
        if (getenv("MEMCACHED_UNIT_TESTS") != NULL) {
            // The crash test just wants to create a coredump within the
            // crash_engine to ensure that breakpad successfuly creates
            // the dump files etc
            if (getenv("MEMCACHED_CRASH_TEST") != NULL) {
                auto engine = createEngine("crash_engine.so",
                                           "create_instance",
                                           logger);
                ENGINE_HANDLE *h;
                if (!engine->createInstance(nullptr, &h)) {
                    *msg = cb_strdup("Failed to create instance of crash engine");
                    return false;
                }
                reinterpret_cast<ENGINE_HANDLE_V1*>(h)->initialize(h, nullptr);
            }
            map[BucketType::EWouldBlock] = createEngine("ewouldblock_engine.so",
                                                        "create_instance",
                                                        logger);
        } else {
            map[BucketType::Couchstore] = createEngine("ep.so",
                                                       "create_instance",
                                                       logger);
        }
    } catch (const std::string &str) {
        *msg = cb_strdup(str.c_str());
        return false;
    }

    return true;
}

BucketType module_to_bucket_type(const char *module)
{
    std::string nm = cb::io::basename(module);
    for (auto entry : map) {
        if (entry.second->getModule() == nm) {
            return entry.first;
        }
    }
    return BucketType::Unknown;
}

void shutdown_engine_map(void)
{
    for (auto entry : map) {
        delete entry.second;
    }
}
