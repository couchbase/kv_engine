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
#include "logger/logger.h"
#include "utilities/engine_loader.h"

#include <platform/cbassert.h>
#include <platform/dirutils.h>
#include <string.h>
#include <iostream>
#include <map>
#include <sstream>
#include <string>

class Engine {
public:
    Engine(const std::string& mod, engine_reference* ref)
        : module(mod), engine_ref(ref) {
        // EMPTY
    }

    ~Engine() {
        unload_engine(engine_ref);
    }

    bool createInstance(GET_SERVER_API get_server_api, EngineIface** handle) {
        return create_engine_instance(engine_ref, get_server_api, handle);
    }

    const std::string &getModule() const {
        return module;
    }

private:
    const std::string module;
    engine_reference* engine_ref;
};

Engine* createEngine(const std::string& so, const std::string& function) {
    auto* engine_ref = load_engine(so.c_str(), function.c_str());

    if (engine_ref == nullptr) {
        throw std::runtime_error("Failed to load engine \"" + so +
                                 "\" with symbol \"" + function + "\"");
    }

    return new Engine(so, engine_ref);
}

std::map<BucketType, Engine*> map;

EngineIface* new_engine_instance(BucketType type,
                                 const std::string& name,
                                 GET_SERVER_API get_server_api) {
    EngineIface* ret = nullptr;
    auto iter = map.find(type);
    cb_assert(iter != map.end());

    if (iter->second->createInstance(get_server_api, &ret)) {
        LOG_INFO(R"(Create bucket "{}" by using "{}")",
                 name,
                 iter->second->getModule());
    }

    return reinterpret_cast<EngineIface*>(ret);
}

void initialize_engine_map() {
    map[BucketType::NoBucket] =
            createEngine("nobucket.so", "create_no_bucket_instance");
    map[BucketType::Memcached] =
            createEngine("default_engine.so", "create_instance");
    map[BucketType::Couchstore] = createEngine("ep.so", "create_instance");
    if (getenv("MEMCACHED_UNIT_TESTS") != NULL) {
        // The crash test just wants to create a coredump within the
        // crash_engine to ensure that breakpad successfuly creates
        // the dump files etc
        if (getenv("MEMCACHED_CRASH_TEST") != NULL) {
            auto engine = createEngine("crash_engine.so", "create_instance");
            EngineIface* h;
            if (!engine->createInstance(nullptr, &h)) {
                delete engine;
                throw std::runtime_error(
                        "initialize_engine_map(): Failed to create instance of "
                        "crash engine");
            }
            h->initialize(nullptr);
            // Not reached, but to mute code analyzers
            delete engine;
        }
        map[BucketType::EWouldBlock] =
                createEngine("ewouldblock_engine.so", "create_instance");
    }
}

BucketType module_to_bucket_type(const char* module) {
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
