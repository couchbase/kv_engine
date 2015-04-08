/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
#include <stdio.h>
#include <cstdlib>
#include <cstring>
#include <iostream>
#include <sstream>

#include <platform/dirutils.h>
#include <extensions/protocol_extension.h>
#include <memcached/config_parser.h>
#include <platform/cbassert.h>

using namespace std;

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
    cb_assert(type == EXTENSION_LOGGER);
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

static void test_rotate(void) {
    EXTENSION_ERROR_CODE ret;
    int ii;

    std::vector<std::string> files;
    files = CouchbaseDirectoryUtilities::findFilesWithPrefix("log_test.rotate");
    if (!files.empty()) {
        remove_files(files);
    }


    // Note: Ensure buffer is at least 4* larger than the expected message
    // length, otherwise the writer will be blocked waiting for the flusher
    // thread to timeout and write (as we haven't actually hit the 75%
    // watermark which would normally trigger an immediate flush).
    ret = memcached_extensions_initialize("unit_test=true;"
            "loglevel=warning;cyclesize=1024;buffersize=512;sleeptime=1;"
            "filename=log_test.rotate", get_server_api);
    cb_assert(ret == EXTENSION_SUCCESS);

    for (ii = 0; ii < 8192; ++ii) {
        logger->log(EXTENSION_LOG_DETAIL, NULL,
                    "Hei hopp, dette er bare noe tull... Paa tide med %05u!!",
                    ii);
    }

    logger->shutdown(false);

    files = CouchbaseDirectoryUtilities::findFilesWithPrefix("log_test");

    // The cyclesize isn't a hard limit. We don't truncate entries that
    // won't fit to move to the next file. We'll rather dump the entire
    // buffer to the file. This means that each file may in theory be
    // up to a buffersize bigger than the cyclesize.. It is a bit hard
    // determine the exact number of files we should get here.. Testing
    // on MacOSX I'm logging 97 bytes in my timezone (summertime), but
    // on Windows it turned out to be a much longer timezone name etc..
    // I'm assuming that we should end up with 90+ files..
    cb_assert(files.size() >= 90);
    remove_files(files);
}

static bool my_fgets(char *buffer, size_t buffsize, FILE *fp) {
    if (fgets(buffer, (int)buffsize, fp) != NULL) {
        char *end = strchr(buffer, '\n');
        cb_assert(end);
        *end = '\0';
        if (*(--end) == '\r') {
            *end = '\0';
        }
    return true;
    } else {
        return false;
    }
}

static std::string create_filename(const std::string &prefix,
                                   const std::string &postfix) {
    std::stringstream ss;
    ss << prefix << "." << time(NULL) << "." << postfix;
    return ss.str();
}

static void test_dedupe(void) {
    EXTENSION_ERROR_CODE ret;
    int ii;
    std::string filename = create_filename("log_test", "dedupe");

    std::vector<std::string> files;
    files = CouchbaseDirectoryUtilities::findFilesWithPrefix(filename);
    if (!files.empty()) {
        remove_files(files);
        files = CouchbaseDirectoryUtilities::findFilesWithPrefix(filename);
        if (!files.empty()) {
            std::cerr << "ERROR: Failed to remove all files: " << std::endl;
            for (auto f : files) {
                std::cerr << "\t" << f << std::endl;
            }
            exit(EXIT_FAILURE);
        }
    }

    std::string config("unit_test=true;"
                       "loglevel=warning;"
                       "cyclesize=1024;"
                       "buffersize=1024;"
                       "sleeptime=1;"
                       "filename=");
    config.append(filename);

    ret = memcached_extensions_initialize(config.c_str(),
                                          get_server_api);
    cb_assert(ret == EXTENSION_SUCCESS);

    for (ii = 0; ii < 1024; ++ii) {
        logger->log(EXTENSION_LOG_DETAIL, NULL,
                    "Hei hopp, dette er bare noe tull... Paa tide med kaffe");
    }

    logger->shutdown(false);

    files = CouchbaseDirectoryUtilities::findFilesWithPrefix(filename);
    if (files.size() != 1) {
        std::cerr << "Expected one file, found " << files.size() << ":"
                  << std::endl;
        for (auto f : files) {
            std::cerr << "\t" << f << std::endl;
        }
        exit(EXIT_FAILURE);
    }

    FILE *fp = fopen(files[0].c_str(), "r");
    if (fp == NULL) {
        std::cerr << "Failed to open " << files[0] << ": "
                  << strerror(errno) << std::endl;
        exit(EXIT_FAILURE);
    }
    char buffer[1024];

    while (my_fgets(buffer, sizeof(buffer), fp)) {
        /* EMPTY */
    }

    if (strstr(buffer, "repeated 1023 times") == NULL) {
        std::cerr << "Incorrect deduplication message:" << std::endl
                  << "Expected to find [repeated 1023 times]"
                  << std::endl
                  << "Found [" << buffer << "]"
                  << std::endl;
        exit(EXIT_FAILURE);
    }

    fclose(fp);
    remove_files(files);
}

int main(int argc, char **argv)
{
    if (argc < 2) {
        std::cerr << "Usage: memcached_logger dedupe|rotate" << std::endl;
        return EXIT_FAILURE;
    }

    if (strcmp(argv[1], "dedupe") == 0) {
        test_dedupe();
    } else if (strcmp(argv[1], "rotate") == 0) {
        test_rotate();
    } else {
        std::cerr << "Usage: memcached_logger dedupe|rotate" << std::endl;
        return EXIT_FAILURE;
    }

    return EXIT_SUCCESS;
}
