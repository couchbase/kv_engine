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

#include "compress.h"

#include <snappy-c.h>

snap_ret_t doSnappyUncompress(const char *buf,
                              size_t len,
                              snap_buf &output) {
    size_t inflated_length;
    if (snappy_uncompressed_length(buf, len, &inflated_length) == SNAPPY_OK) {
        std::unique_ptr<char[]> temp(new char[inflated_length]);
        if (snappy_uncompress(buf, len,
                              temp.get(), &inflated_length) == SNAPPY_OK) {
            output.buf = std::move(temp);
            output.len = inflated_length;
            return SNAP_SUCCESS;
        }
    }
    return SNAP_FAILURE;
}

snap_ret_t doSnappyCompress(const char *buf,
                            size_t len,
                            snap_buf &output) {
    size_t compressed_length = snappy_max_compressed_length(len);
    std::unique_ptr<char[]> temp(new char[compressed_length]);
    if (snappy_compress(buf, len,
                        temp.get(), &compressed_length) == SNAPPY_OK) {
        output.buf = std::move(temp);
        output.len = compressed_length;
        return SNAP_SUCCESS;
    }
    return SNAP_FAILURE;
}
