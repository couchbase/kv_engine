/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2019 Couchbase, Inc
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
