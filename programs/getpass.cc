/* -*- Mode: C; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2017 Couchbase, Inc.
 *
 *   Licensed under the Apache License, Version 2.0 (the "License");
 *   you may not use this file except in compliance with the License.
 *   You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 *   Unless required by applicable law or agreed to in writing, software
 *   distributed under the License is distributed on an "AS IS" BASIS,
 *   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *   See the License for the specific language governing permissions and
 *   limitations under the License.
 */

#include "getpass.h"

#include <iostream>

#ifdef WIN32
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
    struct termios tty;
    tcgetattr(STDIN_FILENO, &tty);
    if(!enable) {
        tty.c_lflag &= ~ECHO;
    } else {
        tty.c_lflag |= ECHO;
    }

    (void) tcsetattr(STDIN_FILENO, TCSANOW, &tty);
#endif
}

std::string getpass(std::string prompt) {
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
