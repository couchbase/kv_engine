/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2010 Couchbase, Inc
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

#include "config.h"

#ifdef _MSC_VER
#define alarm(a)
#endif

#include <cassert>
#include <iostream>

#include "common.h"

static void negativeValidate(const char *s) {
    uint16_t n;
    if (parseUint16(s, &n)) {
        std::cerr << "Expected error parsing " << s
                  << " returned " << n << std::endl;
        exit(1);
    }
}

static void positiveValidate(const char *s, uint16_t expected) {
    uint16_t n;
    if (!parseUint16(s, &n)) {
        std::cerr << "Error parsing " << s << std::endl;
        exit(1);
    }
    assert(n == expected);
}

static void testUint16() {
    negativeValidate("-1");
    negativeValidate("65537");
    negativeValidate("65536");
    positiveValidate("65535", UINT16_MAX);
    positiveValidate("0", 0);
    positiveValidate("32768", 32768);
}

int main(int argc, char **argv) {
    (void)argc; (void)argv;
    alarm(60);
    testUint16();
}
