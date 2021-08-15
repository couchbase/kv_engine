/*
 *    Copyright 2021-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */

#include "terminal_color.h"
#include <atomic>
#include <stdexcept>

std::atomic_bool color_supported{
#ifdef WIN32
        false
#else
        true
#endif
};

void setTerminalColorSupport(bool enable) {
    color_supported.store(enable);
}

static std::string to_string(TerminalColor color) {
    if (!color_supported) {
        return "";
    }
    switch (color) {
    case TerminalColor::Black:
        return "\033[30m";
    case TerminalColor::Red:
        return "\033[31m";
    case TerminalColor::Green:
        return "\033[32m";
    case TerminalColor::Yellow:
        return "\033[33m";
    case TerminalColor::Blue:
        return "\033[34m";
    case TerminalColor::Magenta:
        return "\033[35m";
    case TerminalColor::Cyan:
        return "\033[36m";
    case TerminalColor::White:
        return "\033[37m";
    case TerminalColor::Reset:
        return "\033[m";
    }
    throw std::invalid_argument(
            "to_string(TerminalColor color): Invalid color: " +
            std::to_string(int(color)));
}

std::ostream& operator<<(std::ostream& os, const TerminalColor& color) {
    os << to_string(color);
    return os;
}
