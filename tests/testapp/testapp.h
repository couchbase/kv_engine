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

#include "testapp_binprot.h"

#include <memory>
#include <stdint.h>
#include <stdlib.h>
#include <sys/types.h>
#include <string>

#include <cJSON.h>
#include <gtest/gtest.h>
#include <memcached/protocol_binary.h>
#include <memcached/types.h>

#include "engines/ewouldblock_engine/ewouldblock_engine.h"

#include "testapp_connection.h"

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

// Set of HELLO features which are currently enabled.
extern std::set<protocol_binary_hello_features> enabled_hello_features;

// helper class for use with std::unique_ptr in managing cJSON* objects.
struct cJSONDeleter {
  void operator()(cJSON* j) { cJSON_Delete(j); }
};

typedef std::unique_ptr<cJSON, cJSONDeleter> unique_cJSON_ptr;

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

    static cJSON* generate_config(uint16_t ssl_port);

    static void start_memcached_server(cJSON* config);
    static void start_server(in_port_t *port_out, in_port_t *ssl_port_out,
                             bool daemon, int timeout);

    static void stop_memcached_server();

    // Create the bucket used for testing
    static void CreateTestBucket();

    /* Configure the ewouldblock error-injecting engine */
    static void ewouldblock_engine_configure(ENGINE_ERROR_CODE err_code,
                                             EWBEngine_Mode mode,
                                             uint32_t value);

    /* Disable the ewouldblock_engine. */
    static void ewouldblock_engine_disable();

    // JSON configuration (as JSON object) memcached was configured with.
    static unique_cJSON_ptr memcached_cfg;

    static std::string config_file;

    static ConnectionMap connectionMap;
};

// Test the various memcached binary protocol commands against a
// external `memcached` process. Tests are parameterized to test both Plain and
// SSL transports.
class McdTestappTest : public TestappTest,
                       public ::testing::WithParamInterface<Transport> {
public:
    // Per-test-case set-up.
    // Called before the first test in this test case.
    static void SetUpTestCase();

    // TearDownTestCase same as parent.

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

class McdEnvironment : public ::testing::Environment {
public:

    virtual void SetUp();
    virtual void TearDown();

    const char* getRbacFilename() const {
        return rbac_file_name.c_str();
    }
private:
    std::string isasl_file_name;
    std::string rbac_file_name;
    static char isasl_env_var[256];
};

SOCKET connect_to_server_plain(in_port_t port);

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

/* Attempts to store an object with the given key and value.
 * @param key Document key
 * @param value Document value. Supports up to maximum size server allows.
 * @param validate If true then after storing, read the value from the server
 *        and check it matches the specified value.
 */
void store_object(const char *key, const char *value, bool validate = false);

/* Attempts to delete the object with the given key */
void delete_object(const char *key);

/* Attempts to store an object with a datatype */
enum test_return store_object_w_datatype(const char *key,
                                         const void *data, size_t datalen,
                                         bool deflate, bool json);


// Enables / disables the MUTATION_SEQNO feature.
void set_mutation_seqno_feature(bool enable);

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

int write_config_to_file(const char* config, const char *fname);

void get_working_current_directory(char* out_buf, int out_buf_len);
