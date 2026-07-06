/*
 *     Copyright 2026-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */

#include "vb_filter.h"
#include <boost/range/adaptor/strided.hpp>
#include <gsl/gsl-lite.hpp>
#include <set>
#include <string>
#include <utility>
#include <vector>

std::vector<VBucketFilter> VBucketFilter::split(size_t count) const {
    Expects(!matchAll && "Can't split a match-all VBucketFilter");

    if (count == 0) {
        throw std::invalid_argument("VBucketFilter::split requires count != 0");
    }

    if (count == 1) {
        return {*this};
    }

    // Do not create more filters than there are acceptable vBuckets.
    count = std::min(acceptable.size(), count);

    std::vector<std::set<Vbid>> buckets(count);
    auto filterIndex = 0;
    for (const Vbid& vbid : acceptable) {
        buckets[filterIndex++].insert(vbid);
        filterIndex %= count;
    }

    std::vector<VBucketFilter> filters;
    filters.reserve(count);
    for (auto& s : buckets) {
        filters.push_back(VBucketFilter(std::move(s)));
    }
    return filters;
}

VBucketFilter VBucketFilter::slice(size_t start, size_t stride) const {
    using namespace boost::adaptors;
    Expects(!matchAll && "Can't slice a match-all VBucketFilter");
    Expects(start < stride);
    Expects(start < acceptable.size());

    auto it = acceptable.begin();
    std::advance(it, start);

    std::set<Vbid> vbids;
    for (auto vbid : std::make_pair(it, acceptable.end()) | strided(stride)) {
        vbids.insert(vbid);
    }
    return VBucketFilter{std::move(vbids)};
}

static bool isRange(std::set<Vbid>::const_iterator it,
                    const std::set<Vbid>::const_iterator& end,
                    size_t& length) {
    length = 0;
    for (Vbid val = *it;
         it != end &&
         Vbid(gsl::narrow_cast<Vbid::id_type>(val.get() + length)) == *it;
         ++it, ++length) {
        // empty
    }

    --length;

    return length > 1;
}

std::ostream& operator<<(std::ostream& out, const VBucketFilter& filter) {
    std::set<Vbid>::const_iterator it;

    if (filter.matchAll) {
        out << "{ match-all }";
    } else if (filter.acceptable.empty()) {
        out << "{ empty }";
    } else {
        bool needcomma = false;
        out << "{ ";
        for (it = filter.acceptable.begin(); it != filter.acceptable.end();
             ++it) {
            if (needcomma) {
                out << ", ";
            }

            size_t length;
            if (isRange(it, filter.acceptable.end(), length)) {
                auto last = it;
                for (size_t i = 0; i < length; ++i) {
                    ++last;
                }
                out << "[" << *it << "," << *last << "]";
                it = last;
            } else {
                out << *it;
            }
            needcomma = true;
        }
        out << " }";
    }

    return out;
}
