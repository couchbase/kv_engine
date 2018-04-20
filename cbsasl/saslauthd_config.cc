/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2017 Couchbase, Inc.
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
#include <cbsasl/saslauthd_config.h>
#include <platform/dirutils.h>
#include <platform/rwlock.h>
#include <mutex>
#ifndef WIN32
#include <unistd.h>
#endif

cb::RWLock rwlock;
std::string socketpath;

CBSASL_PUBLIC_API
void cb::sasl::saslauthd::set_socketpath(const std::string& path) {
    std::lock_guard<cb::WriterLock> guard(rwlock.writer());
    socketpath.assign(path);
}

CBSASL_PUBLIC_API
std::string cb::sasl::saslauthd::get_socketpath() {
    std::lock_guard<cb::ReaderLock> guard(rwlock.reader());
    return socketpath;
}

CBSASL_PUBLIC_API
bool cb::sasl::saslauthd::is_configured() {
#ifdef WIN32
    // saslauthd do not work on win32
    return false;
#else
    const auto path = get_socketpath();
    return !(path.empty() || access(path.c_str(), F_OK) == -1);
#endif
}
