/*
 *     Copyright 2025-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */
#include "breakpad.h"
#include "terminate_handler.h"

#include <client/windows/handler/exception_handler.h>
#include <logger/logger.h>
#include <logger/logger_config.h>
#include <platform/backtrace.h>
#include <array>
#include <string>

using namespace google_breakpad;

namespace cb::breakpad::internal {

static void write_to_logger(void* ctx, const char* frame) {
    LOG_CRITICAL("    {}", frame);
}

static bool dumpCallback(const wchar_t* dump_path,
                         const wchar_t* minidump_id,
                         void* context,
                         EXCEPTION_POINTERS* exinfo,
                         MDRawAssertionInfo* assertion,
                         bool succeeded) {
    // Unfortunately the filenames is in wchar's and I got compiler errors
    // from fmt when trying to print them by using {}. Let's just format
    // it into a string first.
    std::array<char, 512> file;
    sprintf(file.data(), "%S/%S.dmp", dump_path, minidump_id);

    LOG_CRITICAL(
            "Breakpad caught a crash (Couchbase version {}). Writing crash "
            "dump to {} before terminating. Writing dump succeeded: {}",
            PRODUCT_VERSION,
            file.data(),
            succeeded ? "yes" : "no");

    if (!succeeded) {
        LOG_CRITICAL("Breakpad failed to write crash dump!");
    }

    LOG_CRITICAL_RAW("Stack backtrace of crashed thread:");
    print_backtrace(write_to_logger, nullptr);
    cb::logger::flush();
    cb::logger::shutdown();
    return succeeded;
}

class WindowsBreakpadInstance : public BreakpadInstance {
public:
    WindowsBreakpadInstance(const std::string& minidump_dir) {
        // Takes a wchar_t* on Windows. Isn't the Breakpad API nice and
        // consistent? ;)
        size_t len = minidump_dir.length() + 1;
        std::wstring wc_minidump_dir(len, '\0');
        size_t wlen = 0;
        mbstowcs_s(&wlen,
                   &wc_minidump_dir[0],
                   len,
                   minidump_dir.c_str(),
                   _TRUNCATE);

        handler.reset(new ExceptionHandler(&wc_minidump_dir[0],
                                           NULL, // filter
                                           dumpCallback,
                                           NULL, // callback context
                                           ExceptionHandler::HANDLER_ALL,
                                           MiniDumpNormal,
                                           (wchar_t*)NULL, // pipe
                                           NULL)); // custom_info
    }

    std::unique_ptr<ExceptionHandler> handler;
};

std::unique_ptr<BreakpadInstance> BreakpadInstance::create(
        const std::string& minidump_dir, const std::string&) {
    return std::make_unique<WindowsBreakpadInstance>(minidump_dir);
}

} // namespace cb::breakpad::internal
