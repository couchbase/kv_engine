/* -*- Mode: C; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2016 Couchbase, Inc.
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
#include <iostream>
#include <cstdlib>

/**
 * As part of MB-16661 server side moxi should be removed. This is a
 * temporary patch that adds a binary named moxi to the system so that
 * ns_server can start and control moxi the way it used to until the
 * ns_server team finds time to prioritize the bug report (this saves
 * us from maintaining the dependencies, source and tests in moxi while
 * we're waiting).
 */
int main(void) {
    std::cerr << "MB-16661: server side moxi should be removed" << std::endl
              << "          Expect failures if you try to use it." << std::endl;

    char buffer[1024];
    std::cin.getline(buffer, sizeof(buffer));
    return EXIT_SUCCESS;
}
