/*
 *    Copyright 2021-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */

#include "terminal_size.h"

#include <system_error>

#ifdef WIN32
#include <windows.h>

std::pair<size_t, size_t> getTerminalSize() {
    CONSOLE_SCREEN_BUFFER_INFO csbi;

    if (GetConsoleScreenBufferInfo(GetStdHandle(STD_OUTPUT_HANDLE), &csbi)) {
        return {csbi.srWindow.Right - csbi.srWindow.Left + 1,
                csbi.srWindow.Bottom - csbi.srWindow.Top + 1};
    }
    throw std::system_error(GetLastError(),
                            std::system_category(),
                            "getTerminalSize(): ioctl failed");
}

#else
#include <sys/ioctl.h>
#include <unistd.h>
#include <limits>

std::pair<size_t, size_t> getTerminalSize() {
    if (isatty(fileno(stdout))) {
        winsize winsize;
        if (ioctl(fileno(stdout), TIOCGWINSZ, &winsize) == -1) {
            throw std::system_error(errno,
                                    std::system_category(),
                                    "getTerminalSize(): ioctl failed");
        }
        return {winsize.ws_col, winsize.ws_row};
    } else {
        // Not a tty
        return {std::numeric_limits<size_t>::max(),
                std::numeric_limits<size_t>::max()};
    }
}
#endif
