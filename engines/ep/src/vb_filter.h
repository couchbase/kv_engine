/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
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

#include <memcached/vbucket.h>

#include <set>
#include <vector>

/**
 * Function object that returns true if the given vbucket is acceptable.
 */
class VBucketFilter {
public:
    /**
     * Instiatiate a VBucketFilter that always returns true.
     */
    VBucketFilter() : acceptable() {
    }

    /**
     * Instantiate a VBucketFilter that returns true for any of the
     * given vbucket IDs.
     */
    explicit VBucketFilter(const std::vector<Vbid>& a)
        : acceptable(a.begin(), a.end()) {
    }

    explicit VBucketFilter(std::set<Vbid> s) : acceptable(std::move(s)) {
    }

    void assign(const std::set<Vbid>& a) {
        acceptable = a;
    }

    bool operator()(Vbid v) const {
        return acceptable.empty() || acceptable.find(v) != acceptable.end();
    }

    bool operator==(const VBucketFilter& other) const {
        return acceptable == other.acceptable;
    }
    bool operator!=(const VBucketFilter& other) const {
        return !(*this == other);
    }

    size_t size() const {
        return acceptable.size();
    }

    bool empty() const {
        return acceptable.empty();
    }

    void reset() {
        acceptable.clear();
    }

    /**
     * Calculate the difference between this and another filter.
     * If "this" contains elements, [1,2,3,4] and other contains [3,4,5,6]
     * the returned filter contains: [1,2,5,6]
     * @param other the other filter to compare with
     * @return a new filter with the elements present in only one of the two
     *         filters.
     */
    VBucketFilter filter_diff(const VBucketFilter& other) const;

    /**
     * Calculate the intersection between this and another filter.
     * If "this" contains elements, [1,2,3,4] and other contains [3,4,5,6]
     * the returned filter contains: [3,4]
     * @param other the other filter to compare with
     * @return a new filter with the elements present in both of the two
     *         filters.
     */
    VBucketFilter filter_intersection(const VBucketFilter& other) const;

    /**
     * Calculate the union between this and another filter.
     * If "this" contains elements, [1,2,3,4] and other contains [3,4,5,6]
     * the returned filter contains: [1,2,3,4,5,6]
     * @param other the other filter to compare with
     * @return a new filter with the elements present in both of the two
     *         filters.
     */
    VBucketFilter filter_union(const VBucketFilter& other) const;

    const std::set<Vbid>& getVBSet() const {
        return acceptable;
    }

    bool addVBucket(Vbid vbucket) {
        auto rv = acceptable.insert(vbucket);
        return rv.second;
    }

    void removeVBucket(Vbid vbucket) {
        acceptable.erase(vbucket);
    }

    /**
     * Distribute the vbuckets in the current filter across @p count separate
     * filters.
     *
     * Each Vbid this filter matches will appear in exactly one of the resulting
     * filters. Vbids are round-robinned between the filters.
     *
     * If @p count exceeds the number of Vbids in the current filter, empty
     * filters will be present in the result, e.g.,
     *
     *  VBucketFilter({1,2,3,4}).split(6);
     *
     * results in 6 filters:
     *
     *  {1}, {2}, {3}, {4}, {}, {}
     *
     */
    std::vector<VBucketFilter> split(size_t count) const;

    /**
     * Dump the filter in a human readable form ( "{ bucket, bucket, bucket }"
     * to the specified output stream.
     */
    friend std::ostream& operator<<(std::ostream& out,
                                    const VBucketFilter& filter);

private:
    std::set<Vbid> acceptable;
};
