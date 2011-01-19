/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
#ifndef PATHEXPAND_HH
#define PATHEXPAND_HH 1

#include "config.h"

#include <string>
#include <cassert>

#include <libgen.h>

/**
 * Expand paths for DB shards.
 *
 * Available variables:
 *
 *   %d - the directory containing the base shard
 *   %b - the basename of the main db
 *   %i - the shard ID
 */
class PathExpander {
public:

    //! Create a path expander with the given main db path.
    PathExpander(const char *p);

    //! Expand the path to the given shard ID.
    std::string expand(const char *pattern, int shardId);

private:

    std::string dir;
    std::string base;
};

#endif // PATHEXPAND_HH
