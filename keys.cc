/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
#include <iostream>
#include <iterator>
#include <set>
#include <vector>
#include <algorithm>
#include <assert.h>
#include <stdlib.h>

#include <string.h>

#include "keys.hh"

#define KEY_LEN 16

namespace kvtest {

    static const char* generateKey();
    static void free_str(const char* p);

    Keys::Keys(size_t n) : keys(n) {

        std::generate_n(keys.begin(), n, generateKey);
        assert(keys.size() == n);

        it = keys.begin();
    }

    Keys::~Keys() {
        std::for_each(keys.begin(), keys.end(), free_str);
    }

    const char* Keys::nextKey() {
        const char *rv = *it;
        ++it;
        if (it == keys.end()) {
            it = keys.begin();
        }
        return rv;
    }

    size_t Keys::length() {
        return keys.size();
    }

    static void free_str(const char* p) {
        free((void*)p);
    }

    static char nextChar() {
        static const char *keyvals = "abcdefghijklmnopqrstuvwyz"
            "ABCDEFGHIJKLMNOPQRSTUVWXYZ"
            "0123456789";
        return keyvals[random() % strlen(keyvals)];
    }

    static const char* generateKey() {
        char *rv = (char *)calloc(1, KEY_LEN + 1);
        assert(rv);
        for (int i = 0; i < KEY_LEN; i++) {
            rv[i] = nextChar();
        }
        return rv;
    }

}
