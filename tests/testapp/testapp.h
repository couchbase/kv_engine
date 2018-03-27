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
#include <tuple>

#include <cJSON.h>
#include <gtest/gtest.h>
#include <memcached/protocol_binary.h>
#include <memcached/types.h>

#include "engines/ewouldblock_engine/ewouldblock_engine.h"

#include <protocol/connection/client_connection.h>
#include <protocol/connection/client_connection_map.h>
#include <protocol/connection/client_mcbp_commands.h>

#include "testapp_environment.h"

enum class TransportProtocols {
    McbpPlain,
    McbpSsl,
    McbpIpv6Plain,
    McbpIpv6Ssl
};

// Properties of a particular subdoc statistic set.
struct SubdocStatTraits {
    const char* count_name;
    const char* bytes_total_name;
    const char* bytes_extracted_subset;
};

namespace Testapp {
const size_t MAX_CONNECTIONS = 1000;
const size_t BACKLOG = 1000;
}

std::ostream& operator<<(std::ostream& os, const TransportProtocols& t);
std::string to_string(const TransportProtocols& transport);

/// Should testapp client negotiate JSON HELLO feature?
enum class ClientJSONSupport { Yes, No };
std::string to_string(ClientJSONSupport json);

/// Should testapp client negotiate Snappy HELLO feature?
enum class ClientSnappySupport { Yes, No };
std::string to_string(ClientSnappySupport s);

// Needed by subdocument tests in seperate .cc file.
extern SOCKET sock;
extern in_port_t port;
extern pid_t server_pid;

// Needed by testapp_tests
extern in_port_t ssl_port;
extern SOCKET sock_ssl;

// Set of HELLO features which are currently enabled.
extern std::set<cb::mcbp::Feature> enabled_hello_features;

class TestBucketImpl;

/**
 * Base test fixture for all 'testapp' tests - aka a test application talking to
 * memcached.
 *
 * These tests connect to memcached via the binary protocol; and issue
 * various commands to test functionality. TestappTest provides baseline
 * functionality for simple tests; or for other more complex subclasses to
 * build on.
 * It provides a single connection type (no SSL / IPv6 etc), and a minimal
 * set of HELLO flags negotiated.
 */
class TestappTest : public ::testing::Test {
public:
    // Per-test-case set-up.
    // Called before the first test in this test case.
    static void SetUpTestCase();

    // Per-test-case tear-down.
    // Called after the last test in this test case.
    static void TearDownTestCase();

    static uint16_t sasl_auth(const char *username, const char *password);

    /// Helper which which returns true if the specified value is correctly
    /// encoded as JSON.
    static bool isJSON(cb::const_char_buffer value);

    /// Does this test/connection support JSON datatype?
    virtual ClientJSONSupport hasJSONSupport() const {
        return ClientJSONSupport::No;
    }

    /// Does this test/connection support Snappy?
    virtual ClientSnappySupport hasSnappySupport() const {
        return ClientSnappySupport::No;
    }

    /**
     * What response datatype do we expect for documents which are JSON?
     * Will be JSON only if the client successfully negotiated JSON feature.
     */
    cb::mcbp::Datatype expectedJSONDatatype() const;

    virtual MemcachedConnection& getConnection();
    virtual MemcachedConnection& getAdminConnection();

protected:
    // per test setup function.
    void SetUp() override;

    // per test tear-down function.
    void TearDown() override;

    //per test compression mode configuration function
    void setCompressionMode(const std::string& compression_mode);

    //per test min compression ratio configuration
    void setMinCompressionRatio(const float min_compression_ratio);

    static unique_cJSON_ptr generate_config(uint16_t ssl_port);
    static unique_cJSON_ptr generate_config();

    static void start_memcached_server(cJSON* config);

    /**
     * Function to start the server and let it listen on a random port.
     * Set <code>server_pid</code> to the pid of the process
     */
    static void start_external_server();

    /**
     * Function to start the server as a thread in the current process
     * and let it listen on a random port.
     *
     * Note that this is only supposed to be used for debugging as it don't
     * properly shut down the server as part of the TearDown process
     * so that you might experience problems if you try to run multiple
     * test batches with the same process..
     *
     * Set <code>server_pid</code> to the pid of the process
     */
    static void spawn_embedded_server();

    /**
     * Parse the portnumber file created from the memcached server
     *
     * @param port_out where to store the TCP port number the server is
     *                 listening on (deprecated, use connectionMap instead)
     * @param ssl_port_out where to store the TCP port number the server is
     *                     listening for SSL connections (deprecated, use
     *                     connectionMap instead)
     */
    static void parse_portnumber_file(in_port_t& port_out,
                                      in_port_t& ssl_port_out);

    static void verify_server_running();

    /**
     * Waits for server to shutdown.  It assumes that the server is
     * already in the process of being shutdown
     * @param killed If true server was shutdown by a signal, and we should
     * expect different waitpid() result.
     */
    static void waitForShutdown(bool killed = false);

    static void stop_memcached_server();

    // Create the bucket used for testing
    static void CreateTestBucket();

    // Delete the bucket used for testing.
    static void DeleteTestBucket();

    // Get information about the bucket used for testing
    static TestBucketImpl& GetTestBucket();

    /* Configure the ewouldblock error-injecting engine */
    static void ewouldblock_engine_configure(ENGINE_ERROR_CODE err_code,
                                             const EWBEngineMode& mode,
                                             uint32_t value,
                                             const std::string& key = "");

    /* Disable the ewouldblock_engine. */
    static void ewouldblock_engine_disable();

    void reconfigure();

    // JSON configuration (as JSON object) memcached was configured with.
    static unique_cJSON_ptr memcached_cfg;
    static std::string portnumber_file;
    static std::string config_file;

    static ConnectionMap connectionMap;
    static uint64_t token;
    static cb_thread_t memcached_server_thread;

    /**
     * Prepare a connection object to be used from a client by reconnecting
     * and performing the initial handshake logic
     *
     * @param connection the connection to prepare
     * @return The connection to use
     */
    MemcachedConnection& prepare(MemcachedConnection& connection);

    /**
     * Create an extended attribute
     *
     * @param path the full path to the attribute (including the key)
     * @param value The value to store
     * @param macro is this a macro for expansion or not
     * @param expectedStatus optional status if success is not expected
     */
    void createXattr(const std::string& path,
                     const std::string& value,
                     bool macro = false);

    void runCreateXattr(const std::string& path,
                        const std::string& value,
                        bool macro,
                        protocol_binary_response_status expectedStatus);

    /**
     * Get an extended attribute
     *
     * @param path the full path to the attribute to fetch
     * @param deleted allow get from deleted documents
     * @param expectedStatus optional status if success is not expected
     * @return the value stored for the key (it is expected to be there!)
     */
    BinprotSubdocResponse getXattr(const std::string& path,
                                   bool deleted = false);

    BinprotSubdocResponse runGetXattr(
            const std::string& path,
            bool deleted,
            protocol_binary_response_status expectedStatus);

    int getResponseCount(protocol_binary_response_status statusCode);

    static int statResps() {
        // Each stats call gets a new connection prepared for it, resulting in
        // a HELLO. This means we expect 1 success from the stats call and
        // the number of successes a HELLO takes.
        return 1 + helloResps();
    }

    static int helloResps() {
        // We do a HELLO single hello enabling all of the features
        // we want as part of preparing the connection.
        return 1;
    }

    static int saslResps() {
        // 2 successes expected due to the initial response and then the
        // continue step.
        return 2;
    }
    std::string name;
    static const std::string bucketName;
};

#define TESTAPP__DOSKIP(cond, reason) \
    if ((cond)) { \
        std::cerr \
        << __FILE__ << ":" << __LINE__ << ": Skipping - '" \
        << #cond << "' (" << reason << ")" << std::endl; \
        return; \
    }

#define TESTAPP_SKIP_IF_UNSUPPORTED(op) \
    do { TESTAPP__DOSKIP(!GetTestBucket().supportsOp(op), #op); } while (0)

#define TESTAPP_SKIP_IF_SUPPORTED(op) \
    do { TESTAPP__DOSKIP(GetTestBucket().supportsOp(op), #op); } while (0)

/**
 * Test fixture for testapp tests which are parameterised on Transport
 * (IPv4/Ipv6,Plain/SSL) and Hello::JSON on/off.
 */
class McdTestappTest
        : public TestappTest,
          public ::testing::WithParamInterface<
                  ::testing::tuple<TransportProtocols, ClientJSONSupport>> {
public:
    /// Custom Test name function.
    static std::string PrintToStringCombinedName(
            const ::testing::TestParamInfo<
                    ::testing::tuple<TransportProtocols, ClientJSONSupport>>&
                    info);

protected:
    // per test setup function.
    void SetUp() override;

    // per test tear-down function.
    void TearDown() override;

    /// return the TransportProtocol parameter for this test instance.
    TransportProtocols getProtocolParam() const {
        return std::get<0>(GetParam());
    }

    /// return the ClientJSONSupport parameter for this test instance.
    ClientJSONSupport getJSONParam() const {
        return std::get<1>(GetParam());
    }

    ClientJSONSupport hasJSONSupport() const override;

    /* Helpers for individual testcases */
    void test_set_huge_impl(const char *key, uint8_t cmd, int result,
                            bool pipeline, int iterations, int message_size);
};

SOCKET connect_to_server_plain(in_port_t port);
void reconnect_to_server();

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

/* Attempts to get the given key and checks if it's flags matches
 * {expected_flags}.
 */
void validate_flags(const char *key, uint32_t expected_flags);

/**
 * Attempts to store a document with the given key, value, flags and expiry
 * time (and optionally compress the value before storing it)
 *
 * @param key Document key
 * @param value Document value. Supports up to maximum size server allows.
 * @param flags Document flag
 * @param exptime Document expiry time
 * @param compress Should the value be compressed before storing
 */
void store_document(const std::string& key,
                    const std::string& value,
                    uint32_t flags = 0,
                    uint32_t exptime = 0,
                    bool compress = false);

/* Attempts to delete the object with the given key.
 * @param key key to remove
 * @param ignore_missing do not fail if key did not exist
 */
void delete_object(const char *key, bool ignore_missing = false);

/**
 * Attempts to store an object with a datatype
 *
 * @param key The documents key
 * @param value The documents value
 * @param flags The documents flags
 * @param expiration The documents expiration (0 == never)
 * @param datatype The datatype to use
 */
void store_object_w_datatype(const std::string& key,
                             cb::const_char_buffer value,
                             uint32_t flags,
                             uint32_t expiration,
                             cb::mcbp::Datatype datatype);

// Enables / disables the JSON feature.
void set_json_feature(bool enable);

// Enables / disables the MUTATION_SEQNO feature.
void set_mutation_seqno_feature(bool enable);

/* Send the specified buffer+len to memcached. */
void safe_send(const void* buf, size_t len, bool hickup);

/* Receive the specified len into buf from memcached */
bool safe_recv(void *buf, size_t len);

/* Attempts to receive size bytes into buf. Returns true if successful.
 */
bool safe_recv_packet(void *buf, size_t size);
bool safe_recv_packet(std::vector<uint8_t>& buf);

/* Whether receiving an EOF during a read is considered an error */
void set_allow_closed_read(bool enabled);

/* The opposite of safe_recv. Simply tries to read from the socket (will use
 * SSL if the socket is SSL configured.
 */
ssize_t phase_recv(void *buf, size_t len);

/* Whether the current socket is SSL */
bool sock_is_ssl();

SOCKET create_connect_plain_socket(in_port_t port);

time_t get_server_start_time();

std::string CERTIFICATE_PATH(const std::string& in);

void write_config_to_file(const std::string& config, const std::string& fname);

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

unique_cJSON_ptr loadJsonFile(const std::string &file);

ssize_t socket_recv(SOCKET s, char *buf, size_t len);
ssize_t socket_send(SOCKET s, const char *buf, size_t len);
void adjust_memcached_clock(int64_t clock_shift, TimeType timeType);
