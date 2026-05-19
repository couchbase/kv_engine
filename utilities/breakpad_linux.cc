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
#include <client/linux/handler/exception_handler.h>

using namespace google_breakpad;

namespace cb::breakpad::internal {

static bool dumpCallback(const MinidumpDescriptor& descriptor,
                         void*,
                         bool succeeded) {
    log_crash_information(descriptor.path(), succeeded);
    return succeeded;
}

class LinuxBreakpadInstance : public BreakpadInstance {
public:
    LinuxBreakpadInstance(const std::string& minidump_dir) {
        MinidumpDescriptor descriptor(minidump_dir);
        handler =
                std::make_unique<ExceptionHandler>(descriptor,
                                                   nullptr, // filter
                                                   dumpCallback,
                                                   nullptr, // callback context
                                                   true, // install_handler
                                                   -1); // server_fg
    }

    std::unique_ptr<ExceptionHandler> handler;
};

std::unique_ptr<BreakpadInstance> BreakpadInstance::create(
        const std::string& minidump_dir) {
    return std::make_unique<LinuxBreakpadInstance>(minidump_dir);
}

} // namespace cb::breakpad::internal
