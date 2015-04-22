/* -*- Mode: C; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
#ifndef ENGINE_LOADER_H
#define ENGINE_LOADER_H

#include <memcached/extension.h>
#include <memcached/engine.h>
#include <memcached/visibility.h>

#ifdef __cplusplus
extern "C" {
#endif

/*
    This type is allocated by load_engine and freed by unload_engine
    and is required to reference the methods exported by the engine.
*/
typedef struct engine_reference engine_reference;

/*
    Unload the engine.
    Triggers destroy_engine then closes the shared object finally freeing the reference.
*/
MEMCACHED_PUBLIC_API void unload_engine(engine_reference* engine);

/*
    Load the specified engine shared object.
    Return a engine_reference* on success or NULL for failure.
*/
MEMCACHED_PUBLIC_API engine_reference* load_engine(const char *soname,
                                                   const char* create_function,
                                                   const char* destroy_function,
                                                   EXTENSION_LOGGER_DESCRIPTOR *logger);

/*
    Create an engine instance.
*/
MEMCACHED_PUBLIC_API bool create_engine_instance(engine_reference* engine,
                                                 SERVER_HANDLE_V1 *(*get_server_api)(void),
                                                 EXTENSION_LOGGER_DESCRIPTOR *logger,
                                                 ENGINE_HANDLE **engine_handle);
/*
    Initialise the engine handle using the engine's exported initialize method.
*/
MEMCACHED_PUBLIC_API bool init_engine_instance(ENGINE_HANDLE *engine,
                                               const char *config_str,
                                               EXTENSION_LOGGER_DESCRIPTOR *logger);

MEMCACHED_PUBLIC_API void log_engine_details(ENGINE_HANDLE *engine,
                                             EXTENSION_LOGGER_DESCRIPTOR *logger);

#ifdef __cplusplus
}
#endif

#endif    /* ENGINE_LOADER_H */
