/*
 *     Copyright 2020-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */

#include <cluster_framework/auth_provider_service.h>
#include <cluster_framework/bucket.h>
#include <cluster_framework/cluster.h>
#include <event2/thread.h>
#include <evhttp.h>
#include <getopt.h>
#include <libevent/utilities.h>
#include <platform/base64.h>
#include <platform/dirutils.h>
#include <platform/socket.h>
#include <programs/getpass.h>
#include <utilities/string_utilities.h>
#include <utilities/terminate_handler.h>
#include <cstdlib>
#include <iostream>
#include <vector>

static std::unique_ptr<cb::test::Cluster> cluster;

/// The credentials used for authentication over the web interface..
static std::string credentials;

/// Method to use to check if the user may do whatever...
/// (@todo figure out where to use this ;)
bool check_credentials(struct evhttp_request* req) {
    auto* headers = evhttp_request_get_input_headers(req);
    if (headers == nullptr) {
        evhttp_send_reply(req, 401, "Unauthorized", nullptr);
        return false;
    }

    auto* auth = evhttp_find_header(headers, "Authorization");
    if (auth == nullptr) {
        evhttp_send_reply(req, 401, "Unauthorized", nullptr);
        return false;
    }

    if (strstr(auth, "Basic ") != auth) {
        evhttp_send_reply(req, 401, "Only basic auth supported", nullptr);
        return false;
    }

    if (credentials == std::string(auth + 6)) {
        return true;
    } else {
        evhttp_send_reply(req, 401, "Unauthorized", nullptr);
        return false;
    }
}

static void add_default_headers(struct evhttp_request* req,
                                const char* contentType,
                                size_t contentLength) {
    auto* headers = evhttp_request_get_output_headers(req);
    evhttp_add_header(
            headers, "Cache-Control", "no-cache,no-store,must-revalidate");
    evhttp_add_header(headers, "Pragma", "no-cache");
    evhttp_add_header(headers, "Content-Type", contentType);
    evhttp_add_header(
            headers, "Content-Length", std::to_string(contentLength).c_str());
    evhttp_add_header(headers, "Server", "Couchbase Server");
}

static void handle_get_pools(struct evhttp_request* req) {
    cb::libevent::unique_evbuffer_ptr buf(evbuffer_new());
    if (!buf) {
        evhttp_send_reply(req, HTTP_INTERNAL, nullptr, nullptr);
        return;
    }

    std::string content;
    if (cluster) {
        content = cluster->to_json().dump();
    } else {
        content = cb::test::Cluster::getUninitializedJson().dump();
    }
    add_default_headers(req, "application/json", content.size());
    evbuffer_add(buf.get(), content.data(), content.size());
    evhttp_send_reply(req, HTTP_OK, "OK", buf.get());
}

static const std::unordered_map<std::string, std::string> decode_encoded_string(
        const std::string& string) {
    std::unordered_map<std::string, std::string> vals;
    for (const auto& str_pair : split_string(string, "&")) {
        auto pair = split_string(str_pair, "=", 1);
        if (pair.size() != 2) {
            throw std::invalid_argument(
                    "decode_encoded_string(): Query pair '" + str_pair +
                    "' did not contain '='");
        } else if (pair[0].empty()) {
            throw std::invalid_argument(
                    "decode_encoded_string(): Query pair had empty argument "
                    "name");
        } else {
            vals.emplace(percent_decode(pair[0]), percent_decode(pair[1]));
        }
    }
    return vals;
}

static void settings_web_callback(struct evhttp_request* req, void*) {
    switch (evhttp_request_get_command(req)) {
    case EVHTTP_REQ_POST:
        break;
    case EVHTTP_REQ_GET:
    case EVHTTP_REQ_HEAD:
    case EVHTTP_REQ_PUT:
    case EVHTTP_REQ_DELETE:
    case EVHTTP_REQ_OPTIONS:
    case EVHTTP_REQ_TRACE:
    case EVHTTP_REQ_CONNECT:
    case EVHTTP_REQ_PATCH:
        evhttp_send_reply(
                req, HTTP_NOTIMPLEMENTED, "Not supported (yet)", nullptr);
        return;

    default:
        evhttp_send_reply(req, HTTP_BADREQUEST, "Invalid command", nullptr);
        return;
    }

    auto buffer = evhttp_request_get_input_buffer(req);
    const auto size = evbuffer_get_length(buffer);
    auto* dataptr = evbuffer_pullup(buffer, size);
    const auto params =
            decode_encoded_string({reinterpret_cast<char*>(dataptr), size});
    auto port = params.find("port");
    if (port != params.cend() && port->second != "6666") {
        evhttp_send_reply(
                req, HTTP_BADREQUEST, "Can't change port base", nullptr);
        return;
    }

    auto username = params.find("username");
    auto password = params.find("password");
    if (username != params.cend()) {
        if (password == params.cend()) {
            evhttp_send_reply(req,
                              HTTP_BADREQUEST,
                              "Can't set username without password",
                              nullptr);
            return;
        }
        credentials = cb::base64::encode(
                username->second + ":" + password->second, false);
        cluster = cb::test::Cluster::create(4);
        cluster->getAuthProviderService().upsertUser(
                {username->second, password->second, R"({
    "buckets": {
      "*": [
        "all"
      ]
    },
    "privileges": [
      "all"
    ],
    "domain": "external"
})"_json});
    } else if (password != params.cend()) {
        evhttp_send_reply(req,
                          HTTP_BADREQUEST,
                          "Can't set password without username",
                          nullptr);
        return;
    } else {
        cluster = cb::test::Cluster::create(4);
    }

    evhttp_send_reply(req, HTTP_OK, "OK", nullptr);
}

static void settings_stats_callback(struct evhttp_request* req, void*) {
    switch (evhttp_request_get_command(req)) {
    case EVHTTP_REQ_POST:
        break;
    case EVHTTP_REQ_GET:
    case EVHTTP_REQ_HEAD:
    case EVHTTP_REQ_PUT:
    case EVHTTP_REQ_DELETE:
    case EVHTTP_REQ_OPTIONS:
    case EVHTTP_REQ_TRACE:
    case EVHTTP_REQ_CONNECT:
    case EVHTTP_REQ_PATCH:
        evhttp_send_reply(
                req, HTTP_NOTIMPLEMENTED, "Not supported (yet)", nullptr);
        return;

    default:
        evhttp_send_reply(req, HTTP_BADREQUEST, "Invalid command", nullptr);
        return;
    }

    auto buffer = evhttp_request_get_input_buffer(req);
    const auto size = evbuffer_get_length(buffer);
    auto* dataptr = evbuffer_pullup(buffer, size);
    std::string data(reinterpret_cast<char*>(dataptr), size);
    if (data != "sendStats=false" && data != "sendStats=true") {
        evhttp_send_reply(req, HTTP_BADREQUEST, "Invalid command", nullptr);
        return;
    }

    evhttp_send_reply(req, HTTP_OK, "OK", nullptr);
}

static void node_controller_setup_services_callback(struct evhttp_request* req,
                                                    void*) {
    switch (evhttp_request_get_command(req)) {
    case EVHTTP_REQ_POST:
        break;
    case EVHTTP_REQ_GET:
    case EVHTTP_REQ_HEAD:
    case EVHTTP_REQ_PUT:
    case EVHTTP_REQ_DELETE:
    case EVHTTP_REQ_OPTIONS:
    case EVHTTP_REQ_TRACE:
    case EVHTTP_REQ_CONNECT:
    case EVHTTP_REQ_PATCH:
        evhttp_send_reply(
                req, HTTP_NOTIMPLEMENTED, "Not supported (yet)", nullptr);
        return;

    default:
        evhttp_send_reply(req, HTTP_BADREQUEST, "Invalid command", nullptr);
        return;
    }

    auto buffer = evhttp_request_get_input_buffer(req);
    const auto size = evbuffer_get_length(buffer);
    auto* dataptr = evbuffer_pullup(buffer, size);
    std::string data(reinterpret_cast<char*>(dataptr), size);
    if (data != "services=kv") {
        evhttp_send_reply(req, HTTP_BADREQUEST, "Invalid command", nullptr);
        return;
    }

    evhttp_send_reply(req, HTTP_OK, "OK", nullptr);
}

static void pools_default_b_callback(struct evhttp_request* req, void*) {
    switch (evhttp_request_get_command(req)) {
    case EVHTTP_REQ_GET:
        break;
    case EVHTTP_REQ_POST:
    case EVHTTP_REQ_HEAD:
    case EVHTTP_REQ_PUT:
    case EVHTTP_REQ_DELETE:
    case EVHTTP_REQ_OPTIONS:
    case EVHTTP_REQ_TRACE:
    case EVHTTP_REQ_CONNECT:
    case EVHTTP_REQ_PATCH:
        evhttp_send_reply(
                req, HTTP_NOTIMPLEMENTED, "Not supported (yet)", nullptr);
        return;

    default:
        evhttp_send_reply(req, HTTP_BADREQUEST, "Invalid command", nullptr);
        return;
    }

    if (!cluster) {
        evhttp_send_reply(
                req, HTTP_BADREQUEST, "Cluster not initialized", nullptr);
        return;
    }

    // get the file name component from the URI
    std::string path(evhttp_uri_get_path(evhttp_request_get_evhttp_uri(req)));
    path = path.substr(path.rfind('/') + 1);
    auto bucket = cluster->getBucket(path);
    if (!bucket) {
        // this shouldn't happen as I installed the handler when I created the
        // bucket
        evhttp_send_reply(req, HTTP_NOTFOUND, "No such bucket", nullptr);
    }
    cb::libevent::unique_evbuffer_ptr buf(evbuffer_new());
    if (!buf) {
        evhttp_send_reply(req, HTTP_INTERNAL, nullptr, nullptr);
        return;
    }

    std::string content = bucket->getManifest().dump(2);
    add_default_headers(req, "application/json", content.size());
    evbuffer_add(buf.get(), content.data(), content.size());
    evhttp_send_reply(req, HTTP_OK, "OK", buf.get());
}

static void pools_default_buckets_callback(struct evhttp_request* req,
                                           void* http) {
    if (!check_credentials(req)) {
        return;
    }
    switch (evhttp_request_get_command(req)) {
    case EVHTTP_REQ_POST:
        break;
    case EVHTTP_REQ_GET:
    case EVHTTP_REQ_HEAD:
    case EVHTTP_REQ_PUT:
    case EVHTTP_REQ_DELETE:
    case EVHTTP_REQ_OPTIONS:
    case EVHTTP_REQ_TRACE:
    case EVHTTP_REQ_CONNECT:
    case EVHTTP_REQ_PATCH:
        evhttp_send_reply(
                req, HTTP_NOTIMPLEMENTED, "Not supported (yet)", nullptr);
        return;

    default:
        evhttp_send_reply(req, HTTP_BADREQUEST, "Invalid command", nullptr);
        return;
    }

    if (!cluster) {
        evhttp_send_reply(
                req, HTTP_BADREQUEST, "Cluster not initialized", nullptr);
        return;
    }

    auto buffer = evhttp_request_get_input_buffer(req);
    const auto size = evbuffer_get_length(buffer);
    auto* dataptr = evbuffer_pullup(buffer, size);
    auto params =
            decode_encoded_string({reinterpret_cast<char*>(dataptr), size});

    // @todo add checks for input params and return errors.. this may crash
    if (params["bucketType"] != "couchbase") {
        evhttp_send_reply(req,
                          HTTP_NOTIMPLEMENTED,
                          "Only couchbase buckets may be used",
                          nullptr);
        return;
    }

    if (params["storageBackend"] != "couchstore") {
        evhttp_send_reply(req,
                          HTTP_NOTIMPLEMENTED,
                          "Only couchstore may be used",
                          nullptr);
        return;
    }

    nlohmann::json config;
    config["max_size"] = std::stoi(params["ramQuotaMB"]) * 1024 * 1024;
    config["replicas"] = std::stoi(params["replicaNumber"]);
    config["compression_mode"] = params["compressionMode"];
    if (params["evictionPolicy"] == "fullEviction") {
        config["item_eviction_policy"] = "full_eviction";
    } else {
        config["item_eviction_policy"] = "value_only";
    }

    // Only use 64 vbuckets as we're running 4 "nodes" on this cluster
    config["max_vbuckets"] = 64;

    auto bucket = cluster->createBucket(params["name"], config, {}, false);
    // @todo This might not be a smart thing to do ;)
    //       I need to kill it when I'm done with it by
    //       using evhttp_del_cb
    std::string terse = "/pools/default/b/" + bucket->getName();
    evhttp_set_cb(static_cast<evhttp*>(http),
                  terse.c_str(),
                  pools_default_b_callback,
                  nullptr);
    evhttp_send_reply(req, HTTP_OK, "OK", nullptr);
}

static void pools_default_callback(struct evhttp_request* req, void*) {
    switch (evhttp_request_get_command(req)) {
    case EVHTTP_REQ_POST:
        break;
    case EVHTTP_REQ_GET:
    case EVHTTP_REQ_HEAD:
    case EVHTTP_REQ_PUT:
    case EVHTTP_REQ_DELETE:
    case EVHTTP_REQ_OPTIONS:
    case EVHTTP_REQ_TRACE:
    case EVHTTP_REQ_CONNECT:
    case EVHTTP_REQ_PATCH:
        evhttp_send_reply(
                req, HTTP_NOTIMPLEMENTED, "Not supported (yet)", nullptr);
        return;

    default:
        evhttp_send_reply(req, HTTP_BADREQUEST, "Invalid command", nullptr);
        return;
    }

    if (cluster) {
        evhttp_send_reply(
                req, HTTP_BADREQUEST, "Cluster already initialized", nullptr);
        return;
    }

    evhttp_send_reply(req, HTTP_OK, "OK", nullptr);
}

static void pools_callback(struct evhttp_request* req, void*) {
    switch (evhttp_request_get_command(req)) {
    case EVHTTP_REQ_GET:
        handle_get_pools(req);
        return;
    case EVHTTP_REQ_POST:
    case EVHTTP_REQ_HEAD:
    case EVHTTP_REQ_PUT:
    case EVHTTP_REQ_DELETE:
    case EVHTTP_REQ_OPTIONS:
    case EVHTTP_REQ_TRACE:
    case EVHTTP_REQ_CONNECT:
    case EVHTTP_REQ_PATCH:
        evhttp_send_reply(
                req, HTTP_NOTIMPLEMENTED, "Not supported (yet)", nullptr);
        break;

    default:
        evhttp_send_reply(req, HTTP_BADREQUEST, "Invalid command", nullptr);
    }
}

int main(int argc, char** argv) {
    install_backtrace_terminate_handler();
    cb::net::initialize();

    bool failed;
#if defined(EVTHREAD_USE_WINDOWS_THREADS_IMPLEMENTED)
    failed = evthread_use_windows_threads() == -1;
#elif defined(EVTHREAD_USE_PTHREADS_IMPLEMENTED)
    failed = evthread_use_pthreads() == -1;
#else
#error "No locking mechanism for libevent available!"
#endif

    if (failed) {
        std::cerr << "Failed to enable libevent locking. Terminating program"
                  << std::endl;
        exit(EXIT_FAILURE);
    }

    int cmd;
    std::string port{"6666"};

    std::vector<option> long_options = {
            {"port", required_argument, nullptr, 'p'},
            {nullptr, 0, nullptr, 0}};

    while ((cmd = getopt_long(argc, argv, "", long_options.data(), nullptr)) !=
           EOF) {
        switch (cmd) {
        case 'p':
            port.assign(optarg);
            break;
        default:
            std::cerr << R"(
usage: kvlite [options]

options:
  --port #     Bind to the provided port

)";
            return EXIT_FAILURE;
        }
    }

    // @todo I need to fix this in a better way ;)
    const auto isasl_file_name = cb::io::sanitizePath(
            SOURCE_ROOT "/tests/testapp_cluster/cbsaslpw.json");

    // Add the file to the exec environment
    static std::array<char, 1024> isasl_env_var;
    snprintf(isasl_env_var.data(),
             isasl_env_var.size(),
             "CBSASL_PWFILE=%s",
             isasl_file_name.c_str());
    putenv(isasl_env_var.data());

    cb::libevent::unique_event_base_ptr base(event_base_new());

    auto* http = evhttp_new(base.get());
    if (evhttp_bind_socket(http, "*", std::atoi(port.c_str())) != 0) {
        std::cerr << "Failed to set up for port: " << port << std::endl;
        return EXIT_FAILURE;
    }

    // check out
    // https://docs.couchbase.com/server/current/rest-api/rest-cluster-intro.html

    evhttp_set_cb(http, "/pools", pools_callback, nullptr);
    evhttp_set_cb(http, "/pools/default", pools_default_callback, nullptr);
    evhttp_set_cb(http,
                  "/pools/default/buckets",
                  pools_default_buckets_callback,
                  http);
    evhttp_set_cb(http,
                  "/node/controller/setupServices",
                  node_controller_setup_services_callback,
                  nullptr);
    evhttp_set_cb(http, "/settings/stats", settings_stats_callback, nullptr);
    evhttp_set_cb(http, "/settings/web", settings_web_callback, nullptr);

    event_base_loop(base.get(), 0);
    evhttp_free(http);

    return EXIT_SUCCESS;
}
