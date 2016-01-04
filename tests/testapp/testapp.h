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

#include <cJSON_utils.h>

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
    SSL,
    PlainIpv6,
    SslIpv6
};

std::ostream& operator << (std::ostream& os, const Transport& t);
const char* to_string(const Transport& transport);

// Needed by subdocument tests in seperate .cc file.
extern SOCKET sock;
extern in_port_t port;
extern pid_t server_pid;

// Set of HELLO features which are currently enabled.
extern std::set<protocol_binary_hello_features> enabled_hello_features;

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

    /**
     * Waits for server to shutdown.  It assumes that the server is
     * already in the process of being shutdown
     */
    static void waitForShutdown();

    static void stop_memcached_server();
    /**
     * Set the session control token in memcached (this token is used
     * to validate the shutdown command)
     */
    static void setControlToken();
    /**
     * Send the shutdown message to the server and read the response
     * back and compare it with the expected result
     */
    static void sendShutdown(protocol_binary_response_status status);

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
    static uint64_t token;
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
void reconnect_to_server();

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

/* Send the specified buffer+len to memcached. */
void safe_send(const void* buf, size_t len, bool hickup);

/* Attempts to receive size bytes into buf. Returns true if successful.
 */
bool safe_recv_packet(void *buf, size_t size);

int write_config_to_file(const char* config, const char *fname);

std::string get_working_current_directory();

// map of statistic key (name) -> value.
typedef std::map<std::string, std::string> stats_response_t;

/* Request stats
 * @return a map of stat key & values in the server response.
 */
stats_response_t request_stats();

/* Extracts a single statistic from the set of stats, returning as a uint64_t
 */
uint64_t extract_single_stat(const stats_response_t& stats,
                                      const char* name);
