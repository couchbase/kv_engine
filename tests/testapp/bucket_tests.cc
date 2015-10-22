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

#include "testapp.h"

TEST_P(McdTestappTest, TestMaxBuckets)
{
    ASSERT_EQ(PROTOCOL_BINARY_RESPONSE_SUCCESS, sasl_auth("_admin",
                                                          "password"));
    union {
        protocol_binary_request_create_bucket request;
        protocol_binary_response_no_extras response;
        char bytes[1024];
    } buffer;

    const char *cfg = "default_engine.so";
    char name[80];

    // We've already created a bucket; "default"
    for (int ii = 1; ii < COUCHBASE_MAX_NUM_BUCKETS; ++ii) {
        snprintf(name, sizeof(name), "mybucket_%03u", ii);
        size_t plen = mcbp_raw_command(buffer.bytes, sizeof(buffer.bytes),
                                       PROTOCOL_BINARY_CMD_CREATE_BUCKET,
                                       name, strlen(name),
                                       cfg, strlen(cfg));

        safe_send(buffer.bytes, plen, false);
        safe_recv_packet(&buffer, sizeof(buffer));

        mcbp_validate_response_header(&buffer.response,
                                      PROTOCOL_BINARY_CMD_CREATE_BUCKET,
                                      PROTOCOL_BINARY_RESPONSE_SUCCESS);

    }

    snprintf(name, sizeof(name), "mybucket_%03u", 0);
    size_t plen = mcbp_raw_command(buffer.bytes, sizeof(buffer.bytes),
                                   PROTOCOL_BINARY_CMD_CREATE_BUCKET,
                                   name, strlen(name),
                                   cfg, strlen(cfg));

    safe_send(buffer.bytes, plen, false);
    safe_recv_packet(&buffer, sizeof(buffer));

    mcbp_validate_response_header(&buffer.response,
                                  PROTOCOL_BINARY_CMD_CREATE_BUCKET,
                                  PROTOCOL_BINARY_RESPONSE_E2BIG);

    for (int ii = 2; ii < COUCHBASE_MAX_NUM_BUCKETS; ++ii) {
        snprintf(name, sizeof(name), "mybucket_%03u", ii);
        size_t plen = mcbp_raw_command(buffer.bytes, sizeof(buffer.bytes),
                                       PROTOCOL_BINARY_CMD_DELETE_BUCKET,
                                       name, strlen(name),
                                       NULL, 0);

        safe_send(buffer.bytes, plen, false);
        safe_recv_packet(&buffer, sizeof(buffer));

        mcbp_validate_response_header(&buffer.response,
                                      PROTOCOL_BINARY_CMD_DELETE_BUCKET,
                                      PROTOCOL_BINARY_RESPONSE_SUCCESS);

    }
}

TEST_P(McdTestappTest, TestBucketIsolationBuckets)
{
    ASSERT_EQ(PROTOCOL_BINARY_RESPONSE_SUCCESS, sasl_auth("_admin",
                                                          "password"));
    union {
        protocol_binary_request_create_bucket request;
        protocol_binary_response_no_extras response;
        char bytes[1024];
    } buffer;

    const char *cfg = "default_engine.so";
    char name[80];

    for (int ii = 2; ii < COUCHBASE_MAX_NUM_BUCKETS; ++ii) {
        snprintf(name, sizeof(name), "mybucket_%03u", ii);
        size_t plen = mcbp_raw_command(buffer.bytes, sizeof(buffer.bytes),
                                       PROTOCOL_BINARY_CMD_CREATE_BUCKET,
                                       name, strlen(name),
                                       cfg, strlen(cfg));

        safe_send(buffer.bytes, plen, false);
        safe_recv_packet(&buffer, sizeof(buffer));

        mcbp_validate_response_header(&buffer.response,
                                      PROTOCOL_BINARY_CMD_CREATE_BUCKET,
                                      PROTOCOL_BINARY_RESPONSE_SUCCESS);

    }

    // I should be able to store the same document in all buckets with
    // ADD

    for (int ii = 2; ii < COUCHBASE_MAX_NUM_BUCKETS; ++ii) {
        snprintf(name, sizeof(name), "mybucket_%03u", ii);
        ASSERT_EQ(PROTOCOL_BINARY_RESPONSE_SUCCESS, sasl_auth(name, name));

        size_t len = mcbp_storage_command(buffer.bytes, sizeof(buffer.bytes),
                                     PROTOCOL_BINARY_CMD_ADD,
                                     "mykey", strlen("mykey"), 0, 0,
                                     0, 0);
        safe_send(buffer.bytes, len, false);
        safe_recv_packet(buffer.bytes, sizeof(buffer.bytes));
        mcbp_validate_response_header(&buffer.response, PROTOCOL_BINARY_CMD_ADD,
                                      PROTOCOL_BINARY_RESPONSE_SUCCESS);
    }

    ASSERT_EQ(PROTOCOL_BINARY_RESPONSE_SUCCESS, sasl_auth("_admin",
                                                          "password"));
    for (int ii = 2; ii < COUCHBASE_MAX_NUM_BUCKETS; ++ii) {
        snprintf(name, sizeof(name), "mybucket_%03u", ii);
        size_t plen = mcbp_raw_command(buffer.bytes, sizeof(buffer.bytes),
                                       PROTOCOL_BINARY_CMD_DELETE_BUCKET,
                                       name, strlen(name),
                                       NULL, 0);

        safe_send(buffer.bytes, plen, false);
        safe_recv_packet(&buffer, sizeof(buffer));

        mcbp_validate_response_header(&buffer.response,
                                      PROTOCOL_BINARY_CMD_DELETE_BUCKET,
                                      PROTOCOL_BINARY_RESPONSE_SUCCESS);

    }
}
