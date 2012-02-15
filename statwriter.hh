#include "config.h"

#include <iostream>

#include <memcached/engine.h>
#include <memcached/protocol_binary.h>

#include "histo.hh"

namespace STATWRITER_NAMESPACE {

void add_casted_stat(const char *k, const char *v,
                            ADD_STAT add_stat, const void *cookie) {
    add_stat(k, static_cast<uint16_t>(strlen(k)),
             v, static_cast<uint32_t>(strlen(v)), cookie);
}

template <typename T>
void add_casted_stat(const char *k, T v,
                            ADD_STAT add_stat, const void *cookie) {
    std::stringstream vals;
    vals << v;
    add_casted_stat(k, vals.str().c_str(), add_stat, cookie);
}

template <typename T>
void add_casted_stat(const char *k, const Atomic<T> &v,
                            ADD_STAT add_stat, const void *cookie) {
    add_casted_stat(k, v.get(), add_stat, cookie);
}

/// @cond DETAILS
/**
 * Convert a histogram into a bunch of calls to add stats.
 */
template <typename T>
struct histo_stat_adder {
    histo_stat_adder(const char *k, ADD_STAT a, const void *c)
        : prefix(k), add_stat(a), cookie(c) {}
    void operator() (const HistogramBin<T>* b) {
        if (b->count()) {
            std::stringstream ss;
            ss << prefix << "_" << b->start() << "," << b->end();
            add_casted_stat(ss.str().c_str(), b->count(), add_stat, cookie);
        }
    }
    const char *prefix;
    ADD_STAT add_stat;
    const void *cookie;
};
/// @endcond

template <typename T>
void add_casted_stat(const char *k, const Histogram<T> &v,
                            ADD_STAT add_stat, const void *cookie) {
    histo_stat_adder<T> a(k, add_stat, cookie);
    std::for_each(v.begin(), v.end(), a);
}

template <typename P, typename T>
void add_prefixed_stat(P prefix, const char *nm, T val,
                  ADD_STAT add_stat, const void *cookie) {
    std::stringstream name;
    name << prefix << ":" << nm;

    add_casted_stat(name.str().c_str(), val, add_stat, cookie);
}

template <typename P, typename T>
void add_prefixed_stat(P prefix, const char *nm, Histogram<T> &val,
                  ADD_STAT add_stat, const void *cookie) {
    std::stringstream name;
    name << prefix << ":" << nm;

    add_casted_stat(name.str().c_str(), val, add_stat, cookie);
}

}

using namespace STATWRITER_NAMESPACE;
