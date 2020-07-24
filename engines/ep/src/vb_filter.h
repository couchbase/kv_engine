/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2017 Couchbase, Inc
 *
 *   Licensed under the Apache License, Version 2.0 (the "License");
 *   you may not use this file except in compliance with the License.
 *   You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 *   Unless required by applicable law or agreed to in writing, software
 *   distributed under the License is distributed on an "AS IS" BASIS,
 *   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *   See the License for the specific language governing permissions and
 *   limitations under the License.
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
    VBucketFilter(const std::vector<Vbid>& a) : acceptable(a.begin(), a.end()) {
    }

    VBucketFilter(std::set<Vbid> s) : acceptable(std::move(s)) {
    }

    void assign(const std::set<Vbid>& a) {
        acceptable = a;
    }

    bool operator()(Vbid v) const {
        return acceptable.empty() || acceptable.find(v) != acceptable.end();
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
     * Dump the filter in a human readable form ( "{ bucket, bucket, bucket }"
     * to the specified output stream.
     */
    friend std::ostream& operator<<(std::ostream& out,
                                    const VBucketFilter& filter);

private:
    std::set<Vbid> acceptable;
};
