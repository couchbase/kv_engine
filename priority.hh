/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
#ifndef PRIORITY_HH
#define PRIORITY_HH

#include "common.hh"

#include <string>

class Priority {
public:
    static const Priority Low;
    static const Priority High;

    bool operator==(const Priority &other) const {
        return (&other == this);
    }

    bool operator<(const Priority &other) const {
        return this == &Low && other == High;
    }

    bool operator>(const Priority &other) const {
        return this == &High && other == Low;
    }

    const std::string &toString() const {
        return name;
    }

    // gcc didn't like the idea of having a class with no constructor
    // available to anyone.. let's make it protected instead to shut
    // gcc up :(
protected:
    Priority(const char *nm) : name(nm) { };
    std::string name;
    DISALLOW_COPY_AND_ASSIGN(Priority);
};

#endif
