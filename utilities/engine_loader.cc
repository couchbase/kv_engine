/* -*- Mode: C; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
#include "config.h"
#include "utilities/engine_loader.h"
#include <ctype.h>
#include <logger/logger.h>
#include <memcached/types.h>
#include <platform/cb_malloc.h>
#include <platform/platform.h>
#include <stdio.h>
#include <string.h>

struct engine_reference {

    /* Union hack to remove a warning from C99 */
    union my_create_u {
        CREATE_INSTANCE create;
        void* voidptr;
    } my_create_instance;

    union my_destroy_u {
        DESTROY_ENGINE destroy;
        void* voidptr;
    } my_destroy_engine;

    cb_dlhandle_t handle;
};

void unload_engine(engine_reference* engine)
{
    if (engine->handle != NULL) {
        (*engine->my_destroy_engine.destroy)();
        cb_dlclose(engine->handle);
        cb_free(engine);
    }
}

static void* find_symbol(cb_dlhandle_t handle, const char* function, char** errmsg) {
    return cb_dlsym(handle, function, errmsg);
}

engine_reference* load_engine(const char* soname, const char* create_function) {
    void *create_symbol = NULL;
    void *destroy_symbol = NULL;
    char *errmsg = NULL, *create_errmsg = NULL, *destroy_errmsg = NULL;
    const char* const create_functions[] = { "create_instance",
                                             "create_default_engine_instance",
                                             "create_ep_engine_instance",
                                             NULL };
    const char* const destroy_functions[] = { "destroy_engine", NULL };

    cb_dlhandle_t handle = cb_dlopen(soname, &errmsg);

    if (handle == NULL) {
        LOG_WARNING(
                R"(Failed to open library "{}": {})", soname, errmsg);
        cb_free(errmsg);
        return NULL;
    }

    if (create_function) {
        create_symbol = find_symbol(handle, create_function, &create_errmsg);
    } else {
        int ii = 0;
        while (create_functions[ii] != NULL && create_symbol == NULL) {
            create_symbol = find_symbol(handle, create_functions[ii], &create_errmsg);
        }
    }

    int ii = 0;
    while (destroy_functions[ii] != NULL && destroy_symbol == NULL) {
        destroy_symbol =
                find_symbol(handle, destroy_functions[ii], &destroy_errmsg);
    }

    if (create_symbol == NULL) {
        LOG_WARNING(
                "Could not find the function to create an engine instance in "
                "{}: {}",
                soname,
                create_errmsg);
        cb_free(create_errmsg);
        return NULL;
    }

    if (destroy_symbol == NULL) {
        LOG_WARNING(
                "Could not find the function to destroy the engine in {}: {}",
                soname,
                destroy_errmsg);
        cb_free(destroy_errmsg);
        return NULL;
    }

    // dlopen is success and we found all required symbols.
    engine_reference* engine_ref = static_cast<engine_reference*>
        (cb_calloc(1, sizeof(engine_reference)));
    engine_ref->handle = handle;
    engine_ref->my_create_instance.voidptr = create_symbol;
    engine_ref->my_destroy_engine.voidptr = destroy_symbol;
    return engine_ref;
}

bool create_engine_instance(engine_reference* engine_ref,
                            SERVER_HANDLE_V1 *(*get_server_api)(void),
                            ENGINE_HANDLE **engine_handle) {
    ENGINE_HANDLE* engine = NULL;

    /* request a instance with protocol version 1 */
    ENGINE_ERROR_CODE error =
            (*engine_ref->my_create_instance.create)(get_server_api, &engine);

    if (error != ENGINE_SUCCESS || engine == NULL) {
        LOG_WARNING("Failed to create instance. Error code: {}", error);
        return false;
    }
    *engine_handle = engine;
    return true;
}

static void logit(const char* field) {
    LOG_WARNING("Failed to initialize engine, missing implementation for {}",
                field);
}

static bool validate_engine_interface(const ENGINE_HANDLE_V1* v1) {
    bool ret = true;
#define check(a)            \
    if (v1->a == nullptr) { \
        logit(#a);          \
        ret = false;        \
    }

    check(store);
    check(store_if);
    check(flush);
    check(get_stats);
    check(reset_stats);
    check(item_set_cas);
    check(get_item_info);
    check(set_item_info);
    check(isXattrEnabled);
#undef check

    return ret;
}

bool init_engine_instance(ENGINE_HANDLE* engine, const char* config_str) {
    ENGINE_ERROR_CODE error;

    if (!validate_engine_interface(engine)) {
        // error already logged
        return false;
    }

    error = engine->initialize(config_str);
    if (error != ENGINE_SUCCESS) {
        engine->destroy(false);
        cb::engine_error err{cb::engine_errc(error),
                             "Failed to initialize instance"};
        LOG_WARNING("{}", err.what());
        return false;
    }
    return true;
}
