/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
#include <stdio.h>
#include <assert.h>
#include <iostream>

#include <platform/dirutils.h>
#include <extensions/protocol_extension.h>
#include <memcached/config_parser.h>

EXTENSION_LOGGER_DESCRIPTOR *logger;

extern "C" {
    static EXTENSION_LOG_LEVEL get_log_level(void);
    static bool register_extension(extension_type_t type, void *extension);
    static void register_callback(ENGINE_HANDLE *eh,
                                  ENGINE_EVENT_TYPE type,
                                  EVENT_CALLBACK cb,
                                  const void *cb_data);
    static SERVER_HANDLE_V1 *get_server_api(void);
}


static EXTENSION_LOG_LEVEL get_log_level(void)
{
    return EXTENSION_LOG_DETAIL;
}

static bool register_extension(extension_type_t type, void *extension)
{
    assert(type == EXTENSION_LOGGER);
    logger = reinterpret_cast<EXTENSION_LOGGER_DESCRIPTOR *>(extension);
    return true;
}

static void register_callback(ENGINE_HANDLE *eh,
                              ENGINE_EVENT_TYPE type,
                              EVENT_CALLBACK cb,
                              const void *cb_data)
{
    (void)eh;
    (void)type;
    (void)cb;
    (void)cb_data;
}

static SERVER_HANDLE_V1 *get_server_api(void)
{
    static int init;
    static SERVER_CORE_API core_api;
    static SERVER_COOKIE_API server_cookie_api;
    static SERVER_STAT_API server_stat_api;
    static SERVER_LOG_API server_log_api;
    static SERVER_EXTENSION_API extension_api;
    static SERVER_CALLBACK_API callback_api;
    static ALLOCATOR_HOOKS_API hooks_api;
    static SERVER_HANDLE_V1 rv;

    if (!init) {
        init = 1;

        core_api.parse_config = parse_config;
        server_log_api.get_level = get_log_level;
        extension_api.register_extension = register_extension;
        callback_api.register_callback = register_callback;

        rv.interface = 1;
        rv.core = &core_api;
        rv.stat = &server_stat_api;
        rv.extension = &extension_api;
        rv.callback = &callback_api;
        rv.log = &server_log_api;
        rv.cookie = &server_cookie_api;
        rv.alloc_hooks = &hooks_api;
    }

    return &rv;
}

static void remove_files(std::vector<std::string> &files) {
    for (std::vector<std::string>::iterator iter = files.begin();
         iter != files.end();
         ++iter) {
        CouchbaseDirectoryUtilities::rmrf(*iter);
    }
}

int main(int argc, char **argv)
{
    EXTENSION_ERROR_CODE ret;
    int ii;

    std::vector<std::string> files;
    files = CouchbaseDirectoryUtilities::findFilesWithPrefix("log_test");
    if (!files.empty()) {
        remove_files(files);
    }


    ret = memcached_extensions_initialize("unit_test=true;prettyprint=true;loglevel=warning;cyclesize=1024;buffersize=128;sleeptime=1;filename=log_test", get_server_api);
    assert(ret == EXTENSION_SUCCESS);

    for (ii = 0; ii < 1024; ++ii) {
        logger->log(EXTENSION_LOG_DETAIL, NULL,
                    "Hei hopp, dette er bare noe tull... Paa tide med kaffe!!");
    }

    logger->shutdown();

    files = CouchbaseDirectoryUtilities::findFilesWithPrefix("log_test");

    // The cyclesize isn't a hard limit. We don't truncate entries that
    // won't fit to move to the next file. We'll rather dump the entire
    // buffer to the file. This means that each file may in theory be
    // up to a buffersize bigger than the cyclesize.. It is a bit hard
    // determine the exact number of files we should get here.. Testing
    // on MacOSX I'm logging 97 bytes in my timezone (summertime), but
    // on Windows it turned out to be a much longer timezone name etc..
    // I'm assuming that we should end up with 90+ files..
    assert(files.size() >= 90);
    remove_files(files);

    return 0;
}
