/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
#ifndef TCMALLOC_STATS_HH
#define TCMALLOC_STATS_HH 1

#include <map>
#include <google/malloc_extension.h>

class TCMallocStats {
public:
    static void getStats(std::map<std::string, size_t> &tc_stats);
};

#endif
