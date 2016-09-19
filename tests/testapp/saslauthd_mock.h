/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2016 Couchbase, Inc.
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

#include <string>
#include <vector>

#ifdef WIN32
#error "This file should not be included on windows"
#endif


class SaslauthdMock {
public:
    SaslauthdMock();

    SaslauthdMock(const SaslauthdMock&) = delete;

    ~SaslauthdMock();

    void processOne();

    const std::string& getSockfile() const {
        return sockfile;
    }

protected:
    void sendResult(int client, const std::string& msg);
    void retrySend(int client, const std::vector<uint8_t>& bytes);

    int sock;

    /**
     * The unix domain socket filename for the saslauthd. This is
     * <code>/tmp/saslauthdmock.[pid]</code>. The reason it is hardcoded
     * to that (and not using mktmp) is that there is a potential race
     * by using mktmp as I have to <b>remove</b> the filename before calling
     * <code>bind</code> and at that time someone else could try to create
     * the file.
     */
    std::string sockfile;
};
