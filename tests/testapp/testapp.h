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

#pragma once

#include "config.h"

#include <stdint.h>
#include <stdlib.h>
#include <sys/types.h>
#include <string>

#include <cJSON.h>
#include <gtest/gtest.h>
#include <memcached/protocol_binary.h>
#include <memcached/types.h>

#include "engines/ewouldblock_engine/ewouldblock_engine.h"

enum test_return { TEST_SKIP, TEST_PASS, TEST_FAIL };

enum class Transport {
    Plain,
    SSL
};

std::ostream& operator << (std::ostream& os, const Transport& t);

// Needed by subdocument tests in seperate .cc file.
extern SOCKET sock;
extern in_port_t port;
extern pid_t server_pid;


// Base class for tests against just the "plain" socker (no SSL).
class TestappTest : public ::testing::Test {
public:
    // Per-test-case set-up.
    // Called before the first test in this test case.
    static void SetUpTestCase();

    // Per-test-case tear-down.
    // Called after the last test in this test case.
    static void TearDownTestCase();


    static uint16_t sasl_auth(const char *username, const char *password);

protected:
    // per test setup function.
    virtual void SetUp();

    // per test tear-down function.
    virtual void TearDown();

    static cJSON* generate_config(int num_threads = -1);

    static void start_memcached_server(cJSON* config);

    // Create the bucket used for testing
    static void CreateTestBucket();

    /* Configure the ewouldblock error-injecting engine */
    static void ewouldblock_engine_configure(ENGINE_ERROR_CODE err_code,
                                             EWBEngine_Mode mode,
                                             uint32_t value);

    /* Disable the ewouldblock_engine. */
    static void ewouldblock_engine_disable();

};

// Test the various memcached binary protocol commands against a
// external `memcached` process. Tests are parameterized to test both Plain and
// SSL transports.
class McdTestappTest : public TestappTest,
                       public ::testing::WithParamInterface<Transport> {
protected:
    // per test setup function.
    virtual void SetUp();

    // per test tear-down function.
    virtual void TearDown();

    /* Helpers for individual testcases */
    void test_set_huge_impl(const char *key, uint8_t cmd, int result,
                            bool pipeline, int iterations, int message_size);

    void test_subdoc_dict_add_cas(bool compress, protocol_binary_command cmd);
};

SOCKET connect_to_server_plain(in_port_t port, bool nonblocking);

/* Compress the given document. Returns the size of the compressed document,
 * and deflated is updated to point to the compressed buffer.
 * 'deflated' should be freed by the caller when no longer needed.
 */
size_t compress_document(const char* data, size_t datalen, char** deflated);

/* Set the datatype feature on the connection to the specified value */
void set_datatype_feature(bool enable);

// Attempts to fetch the document with the given key.
// Returns a pair of {status, value}; where status is the response code from
// the server and value is the documents value (if status == SUCCESS).
std::pair<protocol_binary_response_status, std::string>
fetch_value(const std::string& key);

/* Attempts to get the given key and checks if it's value matches
 * {expected_value}.
 */
void validate_object(const char *key, const std::string& expected_value);

/* Attempts to store an object with the given key and value */
void store_object(const char *key, const char *value);

/* Attempts to delete the object with the given key */
void delete_object(const char *key);

/* Attempts to store an object with a datatype */
enum test_return store_object_w_datatype(const char *key,
                                         const void *data, size_t datalen,
                                         bool deflate, bool json);

/* Populate buf with a binary command with the given parameters. */
off_t raw_command(char* buf, size_t bufsz, uint8_t cmd, const void* key,
                  size_t keylen, const void* dta, size_t dtalen);

/**
 * Constructs a storage command using the give arguments into buf.
 *
 * @param buf the buffer to write the command into
 * @param bufsz the size of the buffer
 * @param cmd the command opcode to use
 * @param key the key to use
 * @param keylen the number of bytes in key
 * @param dta the value for the key
 * @param dtalen the number of bytes in the value
 * @param flags the value to use for the flags
 * @param exp the expiry time
 * @return the number of bytes in the storage command
 */
size_t storage_command(char* buf,
                       size_t bufsz,
                       uint8_t cmd,
                       const void* key,
                       size_t keylen,
                       const void* dta,
                       size_t dtalen,
                       uint32_t flags,
                       uint32_t exp);

/* Send the specified buffer+len to memcached. */
void safe_send(const void* buf, size_t len, bool hickup);

/* Attempts to receive size bytes into buf. Returns true if successful.
 */
bool safe_recv_packet(void *buf, size_t size);

/* Validate the specified response header against the expected cmd and status.
 */
void validate_response_header(protocol_binary_response_no_extras *response,
                              uint8_t cmd, uint16_t status);
