/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2015 Couchbase, Inc
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

#include "config.h"
#include "common.h"
#include "compress.h"

void test_snappy_behavior() {
    std::string send("{\"foo1\":\"bar1\"}");
    snap_buf output1, output2;
    cb_assert(doSnappyUncompress(send.c_str(), send.size(), output1) == SNAP_FAILURE);
    cb_assert(doSnappyCompress(send.c_str(), send.size(), output1) == SNAP_SUCCESS);
    cb_assert(doSnappyUncompress(output1.buf.get(), output1.len, output2) == SNAP_SUCCESS);
    std::string recv(output2.buf.get(), output2.len);
    cb_assert(send == recv);
}

int main(int argc, char **argv) {
    (void)argc;
    (void)argv;

    test_snappy_behavior();
    return 0;
}
