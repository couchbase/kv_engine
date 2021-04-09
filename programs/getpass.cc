/* -*- Mode: C; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2017-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */

#include "getpass.h"

#include <iostream>

#ifdef WIN32
#include <Windows.h>
#define isatty(a) true
#else
#include <termios.h>
#include <unistd.h>
#endif


static void setEcho(bool enable) {
#ifdef WIN32
    HANDLE stdinHandle = GetStdHandle(STD_INPUT_HANDLE);
    DWORD mode;
    GetConsoleMode(stdinHandle, &mode);

    if(!enable) {
        mode &= ~ENABLE_ECHO_INPUT;
    } else {
        mode |= ENABLE_ECHO_INPUT;
    }

    SetConsoleMode(stdinHandle, mode);
#else
    struct termios tty {};
    tcgetattr(STDIN_FILENO, &tty);
    // ECHO is defined as a signed number and we get a warning by using
    // signed variables when doing bit manipulations
    const auto echoflag = tcflag_t(ECHO);
    if(!enable) {
        tty.c_lflag &= ~echoflag;
    } else {
        tty.c_lflag |= echoflag;
    }

    (void) tcsetattr(STDIN_FILENO, TCSANOW, &tty);
#endif
}

std::string getpass(const std::string& prompt) {
    std::string password;
    if (isatty(STDIN_FILENO)) {
        std::cerr << prompt << std::flush;
        setEcho(false);
        std::cin >> password;
        setEcho(true);

        std::cout << std::endl;
    } else {
        std::cin >> password;
    }

    return password;
}
