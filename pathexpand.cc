/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */

#include "config.h"

#include <sstream>

#include "pathexpand.hh"

#ifdef WIN32
const char* path_separator("\\/");
#else
const char* path_separator("/");
#endif

extern const char* path_separator;

static std::string pe_basename(const char *b) {
    assert(b);
    std::string s(b);
    size_t lastthing(s.find_last_of(path_separator));
    if (lastthing == s.npos) {
        return s;
    }

    return s.substr(lastthing + 1);
}

static std::string pe_dirname(const char *b) {
    assert(b);
    std::string s(b);
    size_t lastthing(s.find_last_of(path_separator));
    if (lastthing == s.npos) {
        return std::string(".");
    }

    return s.substr(0, lastthing);
}

PathExpander::PathExpander(const char *p) : dir(pe_dirname(p)),
    base(pe_basename(p)) {
}

std::string PathExpander::expand(const char *pattern, int shardId) {
    std::stringstream ss;

    while (*pattern) {
        if (*pattern == '%') {
            ++pattern;
            switch (*pattern) {
            case 'd':
                ss << dir;
                break;
            case 'b':
                ss << base;
                break;
            case 'i':
                ss << shardId;
                break;
            default:
                ss << '%' << *pattern;
            }
        } else {
            ss << *pattern;
        }
        ++pattern;
    }

    return ss.str();
}
