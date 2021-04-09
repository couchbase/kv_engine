/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2019-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */

#include <memcached/thread_pool_config.h>
#include <ostream>
#include <stdexcept>
#include <string>

ThreadPoolConfig::ThreadPoolConfig(int nr, int nw) {
    if (ThreadCount(nr) < ThreadCount::DiskIOOptimized) {
        throw std::invalid_argument(
                "ThreadPoolConfig: invalid value for num_readers:" +
                std::to_string(nr));
    }
    if (ThreadCount(nw) < ThreadCount::DiskIOOptimized) {
        throw std::invalid_argument(
                "ThreadPoolConfig: invalid value for num_writers:" +
                std::to_string(nw));
    }
    num_readers = ThreadCount(nr);
    num_writers = ThreadCount(nw);
}
std::ostream& operator<<(std::ostream& os,
                         const ThreadPoolConfig::ThreadCount& tc) {
    switch (tc) {
    case ThreadPoolConfig::ThreadCount::DiskIOOptimized:
        os << "DiskIOOptimized";
        return os;
    case ThreadPoolConfig::ThreadCount::Default:
        os << "Default";
        return os;
    default:
        os << static_cast<int>(tc);
        return os;
    }
}
