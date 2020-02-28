/* -*- Mode: C; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */

#include "utilities/engine_loader.h"

#include <logger/logger.h>
#include <memcached/types.h>
#include <platform/dirutils.h>
#include <cctype>
#include <cstdio>

struct engine_reference {
    engine_reference(std::unique_ptr<cb::io::LibraryHandle>& handle,
                     CREATE_INSTANCE create,
                     DESTROY_ENGINE destroy)
        : handle(std::move(handle)), create(create), destroy(destroy) {
    }

    ~engine_reference() {
        destroy();
    }

    std::unique_ptr<cb::io::LibraryHandle> handle;
    const CREATE_INSTANCE create;
    const DESTROY_ENGINE destroy;
};

void unload_engine(engine_reference* engine) {
    delete engine;
}

engine_reference* load_engine(const char* soname, const char* create_function) {
    std::unique_ptr<cb::io::LibraryHandle> handle;
    CREATE_INSTANCE create = nullptr;
    DESTROY_ENGINE destroy = nullptr;
    try {
        handle = cb::io::loadLibrary(soname);
    } catch (const std::exception& e) {
        LOG_WARNING(
                R"(Failed to open library "{}": {})", soname, e.what());
        return nullptr;
    }

    if (create_function) {
        try {
            create = handle->find<CREATE_INSTANCE>(create_function);
        } catch (const std::exception& e) {
            LOG_WARNING(
                    "Could not find the function to create an engine instance "
                    "in {}: {}",
                    soname,
                    e.what());
            return nullptr;
        }
    } else {
        std::vector<std::string> funcs = {{"create_instance",
                                           "create_default_engine_instance",
                                           "create_ep_engine_instance"}};

        for (const auto& func : funcs) {
            try {
                create = handle->find<CREATE_INSTANCE>(func);
                break;
            } catch (const std::exception&) {
                // ignore
            }
        }

        if (create == nullptr) {
            LOG_WARNING(
                    "Could not find the function to create an engine instance "
                    "in {}",
                    soname);
            return nullptr;
        }
    }
    try {
        destroy = handle->find<DESTROY_ENGINE>("destroy_engine");
    } catch (const std::exception& e) {
        LOG_WARNING(
                "Could not find the function to destroy the engine in {}: {}",
                soname,
                e.what());
        return nullptr;
    }

    return new engine_reference(handle, create, destroy);
}

bool create_engine_instance(engine_reference* engine_ref,
                            SERVER_HANDLE_V1* (*get_server_api)(void),
                            EngineIface** engine_handle) {
    EngineIface* engine = nullptr;

    /* request a instance with protocol version 1 */
    const auto error = engine_ref->create(get_server_api, &engine);
    if (error != ENGINE_SUCCESS || engine == nullptr) {
        LOG_WARNING("Failed to create instance. Error code: {}", error);
        return false;
    }
    *engine_handle = engine;
    return true;
}

bool init_engine_instance(EngineIface* engine, const char* config_str) {
    ENGINE_ERROR_CODE error;
    try {
        error = engine->initialize(config_str);
    } catch (const std::exception& e) {
        LOG_WARNING("init_engine_instance: exception caught from initialize {}",
                    e.what());
        error = ENGINE_FAILED;
    }
    if (error != ENGINE_SUCCESS) {
        engine->destroy(false);
        cb::engine_error err{cb::engine_errc(error),
                             "Failed to initialize instance"};
        LOG_WARNING("{}", err.what());
        return false;
    }
    return true;
}
