/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2012 Couchbase, Inc
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

#include <iostream>

#include "common.h"
#include "tools/JSON_checker.h"

#define check(expr, msg) {if(!(expr)) \
    { std::cerr << "JSON test failed: " << msg << std::endl; exit(1); }}
#define CHECK_JSON(X) checkUTF8JSON((const unsigned char *)X, sizeof(X) - 1)
static void testJSON() {
    check(CHECK_JSON("{\"test\": 12}"), "simple json checks as OK");
    check(CHECK_JSON("{\"test\": [[[[[[[[[[[[[[[[[[[[[[12]]]]]]]]]]]]]]]]]]]]]]}"),
            "deep json checks as OK");
    check(!CHECK_JSON("{\"test\": [[[[[[[[[[[[[[[[[[[[[[12]]]]]]]]]]]]]]]]]]]]]]]]}"),
            "bad deep json is not OK");
    check(!CHECK_JSON("{bad stuff}"), "bad json starting with { is not OK");
    check(CHECK_JSON("null"), "bare values are OK");
    check(CHECK_JSON("99"), "bare numbers are OK");
    check(!CHECK_JSON("{\"test\xFF\": 12}"), "bad UTF-8 is not OK");
}

int main(int argc, char **argv) {
    (void)argc; (void)argv;
    testJSON();
}
