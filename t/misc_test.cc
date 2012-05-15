#include "config.h"
#include <cassert>
#include <iostream>

#include "common.hh"

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
