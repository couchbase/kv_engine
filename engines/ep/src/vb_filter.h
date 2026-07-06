/*
 *     Copyright 2017-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */

#pragma once

#include <gsl/gsl-lite.hpp>
#include <memcached/vbucket.h>

#include <set>
#include <vector>

/**
 * Immutable function object that returns true if the given vbucket is
 * acceptable.
 *
 * Filters are immutable after construction: the public API exposes no mutating
 * methods. Construct a new filter instead of modifying an existing one.
 *
 * There are three distinct states:
 *   match-all  (createMatchAll()): operator() returns true for every vbucket.
 *   match-set  (create(...)): operator() returns true only for vbuckets in
 *              the set.
 *   match-none (match-set with an empty set): operator() always returns false.
 *
 * Use isMatchAll() to distinguish between match-all and match-set/match-none,
 * rather than checking getVBSet().size(), which returns 0 for both match-all
 * and match-none.
 *
 * split() and slice() require a match-set filter; calling them on a match-all
 * filter is a precondition violation.
 */
class VBucketFilter {
public:
    /**
     * Create a VBucketFilter that always returns true (match-all).
     */
    static VBucketFilter createMatchAll() {
        return {};
    }

    /**
     * Create a VBucketFilter that returns true for any of the given vbucket
     * IDs. Passing an empty collection creates a filter that matches no
     * vbucket (match-none).
     */
    static VBucketFilter create(const std::vector<Vbid>& a) {
        return VBucketFilter(a);
    }

    /**
     * Create a VBucketFilter that returns true for any of the given vbucket
     * IDs. Passing an empty collection creates a filter that matches no
     * vbucket (match-none).
     */
    static VBucketFilter create(std::set<Vbid> s) {
        return VBucketFilter(std::move(s));
    }

    /**
     * Create a VBucketFilter that returns true for the provided vbucket
     */
    static VBucketFilter create(Vbid vb) {
        return VBucketFilter(std::set{vb});
    }

    bool operator()(Vbid v) const {
        return matchAll || acceptable.find(v) != acceptable.end();
    }

    bool operator==(const VBucketFilter& other) const = default;

    /**
     * Returns true when this filter matches every vbucket (match-all state).
     */
    bool isMatchAll() const {
        return matchAll;
    }

    /**
     * Returns the set of acceptable vbuckets.
     * Returns an empty set for both match-all and match-none; use isMatchAll()
     * to distinguish those cases. Use getVBSet().size() to count vbuckets in
     * match-set mode.
     */
    const std::set<Vbid>& getVBSet() const {
        Expects(!matchAll &&
                "A match-all filter does not have an underlying vbset");
        return acceptable;
    }

    /**
     * Distribute the vbuckets in the current filter across @p count separate
     * filters.
     *
     * Each Vbid this filter matches will appear in exactly one of the resulting
     * filters. Vbids are round-robinned between the filters.
     *
     *  VBucketFilter::create({1,2,3,4}).split(6);
     *
     * results in 4 filters:
     *
     *  {1}, {2}, {3}, {4}
     *
     * Precondition: the filter must be in match-set mode (!isMatchAll()).
     */
    std::vector<VBucketFilter> split(size_t count) const;

    /**
     * Create a new filter by selecting every (start + i * stride) item.
     *
     * VBucketFilter::create({0,1,2,3,4,5,6,7,8,9}).slice(2, 3) -> {2,5,8}
     *
     * Precondition: the filter must be in match-set mode (!isMatchAll()).
     */
    VBucketFilter slice(size_t start, size_t stride = 1) const;

    /**
     * Write the filter to @p out in human-readable form:
     *   match-all:  "{ match-all }"
     *   match-none: "{ empty }"
     *   match-set:  "{ vb:N, vb:M, [vb:X,vb:Y], ... }"
     *               (consecutive runs of 3+ vbuckets are compressed to ranges)
     */
    friend std::ostream& operator<<(std::ostream& out,
                                    const VBucketFilter& filter);

private:
    VBucketFilter() : matchAll(true) {
    }

    explicit VBucketFilter(const std::vector<Vbid>& a)
        : matchAll(false), acceptable(a.begin(), a.end()) {
    }

    explicit VBucketFilter(std::set<Vbid> s)
        : matchAll(false), acceptable(std::move(s)) {
    }

    bool matchAll;
    std::set<Vbid> acceptable;
};
