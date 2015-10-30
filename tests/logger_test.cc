/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */

#include "logger_test_common.h"

#include <platform/cbassert.h>
#include <platform/dirutils.h>

#include <cstring>
#include <cstdio>
#include <iostream>
#include <sstream>
#include <vector>

int main(int argc, char **argv)
{
    EXTENSION_ERROR_CODE ret;
    int ii;

    std::vector<std::string> files;
    files = CouchbaseDirectoryUtilities::findFilesWithPrefix("log_test");
    if (!files.empty()) {
        remove_files(files);
    }


    // Note: Ensure buffer is at least 4* larger than the expected message
    // length, otherwise the writer will be blocked waiting for the flusher
    // thread to timeout and write (as we haven't actually hit the 75%
    // watermark which would normally trigger an immediate flush).
    ret = memcached_extensions_initialize("unit_test=true;prettyprint=true;loglevel=warning;cyclesize=1024;buffersize=512;sleeptime=1;filename=log_test", get_server_api);
    assert(ret == EXTENSION_SUCCESS);

    for (ii = 0; ii < 8192; ++ii) {
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
