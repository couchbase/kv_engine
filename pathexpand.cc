/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */

#include "config.h"

#include <sstream>

#include "pathexpand.hh"

#ifdef WIN32
const char* path_separator("\\/");
#else
const char* path_separator("/");
#endif

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
