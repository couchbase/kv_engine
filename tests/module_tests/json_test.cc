#include "config.h"

#include <cassert>
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
