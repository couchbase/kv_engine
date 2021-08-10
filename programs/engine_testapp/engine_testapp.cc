/*
 *     Copyright 2021-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */

#include "mock_cookie.h"
#include "mock_engine.h"
#include "mock_server.h"
#include "utilities/terminate_handler.h"

#include <daemon/enginemap.h>
#include <executor/executorpool.h>
#include <folly/ScopeGuard.h>
#include <getopt.h>
#include <logger/logger.h>
#include <memcached/dcp.h>
#include <memcached/durability_spec.h>
#include <memcached/engine_testapp.h>
#include <memcached/server_cookie_iface.h>
#include <phosphor/phosphor.h>
#include <platform/cbassert.h>
#include <platform/dirutils.h>
#include <platform/socket.h>
#include <utilities/string_utilities.h>
#include <chrono>
#include <cinttypes>
#include <cstdlib>
#include <map>
#include <memory>
#include <regex>
#include <string>
#include <vector>

static bool color_enabled;
static bool verbose_logging = false;

// The handle for the 'current' engine, as used by execute_test.
// It needs to be globalas the testcase may call reload_engine() and that
// needs to update the pointers the new engine, so when execute_test is
// cleaning up it has the correct handle.
static EngineIface* currentEngineHandle = nullptr;

static void usage() {
    printf("\n");
    printf("engine_testapp -E <ep|mc>\n");
    printf("               [-e <engine_config>] [-h] [-X]\n");
    printf("\n");
    printf("-E <ep|mc>                   The engine to use.\n");
    printf("                               ep = ep-engine\n");
    printf("                               mc = default/memcache\n");
    printf("\n");
    printf("-a <attempts>                Maximum number of attempts for a test.\n");
    printf("-e <engine_config>           Engine configuration string passed to\n");
    printf("                             the engine.\n");
    printf("-q                           Only print errors.");
    printf("-.                           Print a . for each executed test.");
    printf("\n");
    printf("-h                           Prints this usage text.\n");
    printf("-v                           verbose output\n");
    printf("-X                           Use stderr logger instead of /dev/zero\n");
    printf("-n                           Regex specifying name(s) of test(s) to run\n");
}

static int report_test(const char* name,
                       std::chrono::steady_clock::duration duration,
                       enum test_result r,
                       bool quiet,
                       bool compact) {
    int rc = 0;
    const char* msg = nullptr;
    int color = 0;
    char color_str[8] = { 0 };
    const char *reset_color = color_enabled ? "\033[m" : "";

    switch (r) {
    case SUCCESS:
        msg="OK";
        color = 32;
        break;
    case SKIPPED:
        msg="SKIPPED";
        color = 32;
        break;
    case FAIL:
        color = 31;
        msg="FAIL";
        rc = 1;
        break;
    case DIED:
        color = 31;
        msg = "DIED";
        rc = 1;
        break;
    case PENDING:
        color = 33;
        msg = "PENDING";
        break;
    case SUCCESS_AFTER_RETRY:
        msg="OK AFTER RETRY";
        color = 33;
        break;
    case SKIPPED_UNDER_ROCKSDB:
        msg="SKIPPED_UNDER_ROCKSDB";
        color = 32;
        break;
    case SKIPPED_UNDER_MAGMA:
        msg = "SKIPPED_UNDER_MAGMA";
        color = 32;
        break;
    default:
        color = 31;
        msg = "UNKNOWN";
        rc = 1;
    }

    cb_assert(msg);
    if (color_enabled) {
        snprintf(color_str, sizeof(color_str), "\033[%dm", color);
    }

    auto duration_ms = std::chrono::duration_cast<std::chrono::milliseconds>(duration);
    if (quiet) {
        if (r != SUCCESS) {
            printf("%s:  (%" PRIu64 " ms) %s%s%s\n", name, duration_ms.count(),
                   color_str, msg, reset_color);
            fflush(stdout);
        }
    } else {
        if (compact && (r == SUCCESS || r == SKIPPED || r == PENDING)) {
            size_t len = strlen(name) + 27; /* for "Running [0/0] xxxx ..." etc */
            size_t ii;

            fprintf(stdout, "\r");
            for (ii = 0; ii < len; ++ii) {
                fprintf(stdout, " ");
            }
            fprintf(stdout, "\r");
            fflush(stdout);
        } else {
            printf("(%" PRIu64 " ms) %s%s%s\n", duration_ms.count(), color_str,
                   msg, reset_color);
        }
    }
    return rc;
}

class MockTestHarness : public test_harness {
public:
    CookieIface* create_cookie(EngineIface* engine) override {
        return create_mock_cookie(engine);
    }

    void destroy_cookie(CookieIface* cookie) override {
        destroy_mock_cookie(cookie);
    }

    void set_ewouldblock_handling(const CookieIface* cookie,
                                  bool enable) override {
        const_cast<CookieIface*>(cookie)->setEwouldblock(enable);
    }

    void set_mutation_extras_handling(const CookieIface* cookie,
                                      bool enable) override {
        dynamic_cast<MockCookie*>(const_cast<CookieIface*>(cookie))
                ->setMutationExtrasHandling(enable);
    }

    void set_datatype_support(const CookieIface* cookie,
                              protocol_binary_datatype_t datatypes) override {
        dynamic_cast<MockCookie*>(const_cast<CookieIface*>(cookie))
                ->setDatatypeSupport(datatypes);
    }

    void set_collections_support(const CookieIface* cookie,
                                 bool enable) override {
        dynamic_cast<MockCookie*>(const_cast<CookieIface*>(cookie))
                ->setCollectionsSupport(enable);
    }

    void lock_cookie(const CookieIface* cookie) override {
        dynamic_cast<MockCookie*>(const_cast<CookieIface*>(cookie))->lock();
    }

    void unlock_cookie(const CookieIface* cookie) override {
        dynamic_cast<MockCookie*>(const_cast<CookieIface*>(cookie))->unlock();
    }

    void waitfor_cookie(const CookieIface* cookie) override {
        dynamic_cast<MockCookie*>(const_cast<CookieIface*>(cookie))->wait();
    }

    int get_number_of_mock_cookie_references(
            const CookieIface* cookie) override {
        return const_cast<CookieIface*>(cookie)->getRefcount();
    }

    void set_pre_link_function(PreLinkFunction function) override {
        mock_set_pre_link_function(function);
    }

    void time_travel(int offset) override {
        mock_time_travel(offset);
    }

    void set_current_testcase(engine_test_t* testcase) {
        current_testcase = testcase;
    }

    const engine_test_t* get_current_testcase() override {
        return current_testcase;
    }

    EngineIface* create_bucket(bool initialize,
                               const std::string& cfg) override {
        auto me = std::make_unique<MockEngine>(
                new_engine_instance(bucketType, &get_mock_server_api));
        if (me) {
            if (initialize) {
                const auto error = me->the_engine->initialize(
                        cfg.empty() ? nullptr : cfg.c_str());
                if (error != cb::engine_errc::success) {
                    me->the_engine->destroy(false /*force*/);
                    throw cb::engine_error{cb::engine_errc(error),
                                           "Failed to initialize instance"};
                }
            }
        }
        return me.release();
    }

    void destroy_bucket(EngineIface* handle, bool force) override {
        // destroy should delete the handle
        handle->destroy(force);

        // If:
        //  - a test calls reload_engine
        //  - the call throws from create_bucket
        //  - the throw is success condition for the test
        // then:
        //  - we return SUCCESS from the test
        //  - execute_test tries to destroy_bucket again as
        //    currentEngineHandle != nullptr
        //
        // @todo: It seems that currentEngineHandle is the only handle around
        //  at every run, can we remove the handle arg from this function?
        if (handle == currentEngineHandle) {
            currentEngineHandle = nullptr;
        }
    }

    void reload_engine(EngineIface** h,
                       const std::string& cfg,
                       bool init,
                       bool force) override {
        destroy_bucket(*h, force);
        currentEngineHandle = *h = create_bucket(init, cfg);
    }

    void notify_io_complete(const CookieIface* cookie,
                            cb::engine_errc status) override {
        get_mock_server_api()->cookie->notify_io_complete(*cookie, status);
    }

private:
    engine_test_t* current_testcase = nullptr;
};

MockTestHarness harness;

/**
 * Attempts to run the given test function; returning the status code returned
 * from test func, or if an exception is thrown then catch and return a failure
 * code
 */
test_result try_run_test(std::function<test_result()> testFunc) noexcept {
    try {
        return testFunc();
    } catch (const TestExpectationFailed&) {
        return FAIL;
    } catch (const std::exception& e) {
        fprintf(stderr, "Uncaught std::exception. what():%s\n", e.what());
        return DIED;
    } catch (...) {
        // This is a non-test exception (i.e. not an explicit test check which
        // failed) - mark as "died".
        return DIED;
    }
}

static test_result execute_test(engine_test_t test,
                                const char* engine,
                                const char* default_cfg) {
    auto executorBoarderGuard =
            folly::makeGuard([] { ExecutorPool::shutdown(); });

    enum test_result ret = PENDING;
    cb_assert(test.tfun != nullptr || test.api_v2.tfun != nullptr);
    bool test_api_1 = test.tfun != nullptr;

    /**
     * Combine test.cfg (internal config parameters) and
     * default_cfg (command line parameters) for the test case.
     *
     * default_cfg will have higher priority over test.cfg in
     * case of redundant parameters.
     */
    std::string cfg;
    if (!test.cfg.empty()) {
        if (default_cfg != nullptr) {
            cfg.assign(test.cfg);
            cfg = cfg + ";" + default_cfg + ";";
            std::string token, delimiter(";");
            std::string::size_type i, j;
            std::map<std::string, std::string> map;

            while (!cfg.empty() &&
                   (i = cfg.find(delimiter)) != std::string::npos) {
                std::string temp(cfg.substr(0, i));
                cfg.erase(0, i + 1);
                j = temp.find('=');
                if (j == std::string::npos) {
                    continue;
                }
                std::string k(temp.substr(0, j));
                std::string v(temp.substr(j + 1, temp.size()));
                map[k] = v;
            }
            cfg.clear();
            std::map<std::string, std::string>::iterator it;
            for (it = map.begin(); it != map.end(); ++it) {
                cfg = cfg + it->first + "=" + it->second + ";";
            }
            test.cfg = std::move(cfg);
        }
    } else if (default_cfg != nullptr) {
        test.cfg = default_cfg;
    }

    // Necessary configuration to run tests under RocksDB.
    if (!test.cfg.empty()) {
        cfg.assign(test.cfg);
        if (std::string(test.cfg).find("backend=rocksdb") !=
            std::string::npos) {
            if (!cfg.empty() && cfg.back() != ';') {
                cfg.append(";");
            }
            // MB-26973: Disable RocksDB pre-allocation of disk space by
            // default. When 'allow_fallocate=true', RocksDB pre-allocates disk
            // space for the MANIFEST and WAL files (some tests showed up to
            // ~75MB per DB, ~7.5GB for 100 empty DBs created).
            cfg.append("rocksdb_options=allow_fallocate=false;");
            // BucketQuota is now used to calculate the MemtablesQuota at
            // runtime. The baseline value for BucketQuota is taken from the
            // 'max_size' default value in configuration.json. If that default
            // value is 0, then EPEngine sets the value to 'size_t::max()',
            // leading to a huge MemtablesQuota. Avoid that 'size_t::max()' is
            // used in the computation for MemtablesQuota.
            if (cfg.find("max_size") == std::string::npos) {
                cfg.append("max_size=1073741824;");
            }
            test.cfg = std::move(cfg);
        } else if (std::string(test.cfg).find("backend=magma") !=
                   std::string::npos) {
            if (!cfg.empty() && cfg.back() != ';') {
                cfg.append(";");
            }
            // The way magma set its memory quota is to use 10% of the
            // max_size per shard. Set this to allow for 3MB per shard assuming
            // there are 4 shards.
            // 3145728 * 4 / 0.1 = 125829120
            if (cfg.find("max_size") == std::string::npos) {
                cfg.append("max_size=125829120;");
            }
            test.cfg = std::move(cfg);
        }
    }

    auto executorBackend = ExecutorPool::Backend::Default;
    int executorNumMaxThread = 0;
    int executorNumAuxIo = 0;
    int executorNumNonIo = 0;
    auto options = split_string(test.cfg, ";");
    for (auto& o : options) {
        auto kv = split_string(o, "=");
        if (kv.front() == "executor_pool_backend") {
            if (kv.back() == "cb3") {
                executorBackend = ExecutorPool::Backend::CB3;
            }
        } else if (kv.front() == "max_threads") {
            executorNumMaxThread = std::stoi(kv.back());
        } else if (kv.front() == "num_auxio_threads") {
            executorNumAuxIo = std::stoi(kv.back());
        } else if (kv.front() == "num_nonio_threads") {
            executorNumNonIo = std::stoi(kv.back());
        }
    }

    ExecutorPool::create(executorBackend,
                         executorNumMaxThread,
                         ThreadPoolConfig::ThreadCount(0),
                         ThreadPoolConfig::ThreadCount(0),
                         executorNumAuxIo,
                         executorNumNonIo);

    harness.set_current_testcase(&test);
    if (test.prepare != nullptr) {
        if ((ret = test.prepare(&test)) == SUCCESS) {
            ret = PENDING;
        }
    }

    if (ret == PENDING) {
        init_mock_server();

        const auto spd_log_level =
                verbose_logging ? spdlog::level::level_enum::debug
                                : spdlog::level::level_enum::critical;
        cb::logger::get()->set_level(spd_log_level);

        if (test_api_1) {
            // all test (API1) get 1 bucket and they are welcome to ask for more.
            currentEngineHandle = harness.create_bucket(
                    true,
                    test.cfg.empty() ? (default_cfg ? default_cfg : "")
                                     : test.cfg);
            if (test.test_setup != nullptr &&
                !test.test_setup(currentEngineHandle)) {
                fprintf(stderr,
                        "Failed to run setup for test %s\n",
                        test.name.c_str());
                return FAIL;
            }

            ret = try_run_test([&] { return test.tfun(currentEngineHandle); });

            if (test.test_teardown != nullptr &&
                !test.test_teardown(currentEngineHandle)) {
                fprintf(stderr,
                        "WARNING: Failed to run teardown for test %s\n",
                        test.name.c_str());
            }

        } else {
            if (test.api_v2.test_setup != nullptr &&
                !test.api_v2.test_setup(&test)) {
                fprintf(stderr,
                        "Failed to run setup for test %s\n",
                        test.name.c_str());
                return FAIL;
            }

            ret = try_run_test([&] { return test.api_v2.tfun(&test); });

            if (test.api_v2.test_teardown != nullptr &&
                !test.api_v2.test_teardown(&test)) {
                fprintf(stderr,
                        "WARNING: Failed to run teardown for test %s\n",
                        test.name.c_str());
            }
        }

        if (currentEngineHandle) {
            // If test failed then bucket is in unknown state - force shutdown
            // in such a case.
            const bool force = ret != SUCCESS;
            harness.destroy_bucket(currentEngineHandle, force);
            currentEngineHandle = nullptr;
        }

        shutdown_all_engines();
        PHOSPHOR_INSTANCE.stop();

        if (test.cleanup) {
            test.cleanup(&test, ret);
        }
    }

    return ret;
}

int main(int argc, char **argv) {
    int c, exitcode = 0, loop_count = 0;
    bool verbose = false;
    bool quiet = false;
    bool dot = false;
    bool loop = false;
    bool terminate_on_error = false;
    std::string engine;
    const char* engine_args = nullptr;
    std::unique_ptr<std::regex> test_case_regex;
    std::vector<engine_test_t> testcases;
    int test_case_id = -1;

    /* If a testcase fails, retry up to 'attempts -1' times to allow it
       to pass - this is here to allow us to deal with intermittant
       test failures without having to manually retry the whole
       job. */
    int attempts = 1;

    setupWindowsDebugCRTAssertHandling();
    cb::logger::createConsoleLogger();
    cb::net::initialize();

    auto limit = cb::io::maximizeFileDescriptors(1024);
    if (limit < 1024) {
        std::cerr << "Error: The unit tests needs at least 1k file descriptors"
                  << std::endl;
        exit(EXIT_FAILURE);
    }

    color_enabled = getenv("TESTAPP_ENABLE_COLOR") != nullptr;

    /* Allow 'attempts' to also be set via env variable - this allows
       commit-validation scripts to enable retries for all
       engine_testapp-driven tests trivually. */
    const char* attempts_env;
    if ((attempts_env = getenv("TESTAPP_ATTEMPTS")) != nullptr) {
        attempts = std::stoi(attempts_env);
    }

    /* Use unbuffered stdio */
    setbuf(stdout, nullptr);
    setbuf(stderr, nullptr);

    install_backtrace_terminate_handler();

    /* process arguments */
    while ((c = getopt(
                    argc,
                    argv,
                    "a:" /* attempt tests N times before declaring them failed
                          */
                    "h" /* usage */
                    "E:" /* Engine to use */
                    "e:" /* Engine options */
                    "L" /* Loop until failure */
                    "q" /* Be more quiet (only report failures) */
                    "." /* dot mode. */
                    "n:" /* regex for test case(s) to run */
                    "v" /* verbose output */
                    "Z" /* Terminate on first error */
                    "C:" /* Test case id */
                    "s" /* spinlock the program */
                    "X" /* Use stderr logger */
                    "f:" /* output format. Valid values are: 'text' and 'xml' */
                    )) != -1) {
        switch (c) {
        case 'a':
            attempts = std::stoi(optarg);
            break;
        case 's' : {
            int spin = 1;
            while (spin) {

            }
            break;
        }
        case 'C' :
            test_case_id = std::stoi(optarg);
            break;
        case 'E':
            engine = optarg;
            break;
        case 'e':
            engine_args = optarg;
            break;
        case 'f':
            if (std::string(optarg) == "text") {
                harness.output_format = OutputFormat::Text;
            } else if (std::string(optarg) == "xml") {
                harness.output_format = OutputFormat::XML;
            } else {
                fprintf(stderr, "Invalid option for output format '%s'. Valid "
                    "options are 'text' and 'xml'.\n", optarg);
                return 1;
            }
            break;
        case 'h':
            usage();
            return 0;
        case 'L':
            loop = true;
            break;
        case 'n':
            test_case_regex = std::make_unique<std::regex>(optarg);
            break;
        case 'v' :
            verbose = true;
            break;
        case 'q':
            quiet = true;
            break;
        case '.':
            dot = true;
            break;
        case 'Z' :
            terminate_on_error = true;
            break;
        case 'X':
            verbose_logging = true;
            break;
        default:
            fprintf(stderr, "Illegal argument \"%c\"\n", c);
            return 1;
        }
    }

    /* validate args */
    if (engine.empty()) {
        harness.bucketType = get_bucket_type();
    } else {
        if (engine == "ep") {
            harness.bucketType = BucketType::Couchbase;
        } else if (engine == "mc") {
            harness.bucketType = BucketType::Memcached;
        } else {
            fprintf(stderr, R"(Engine must be "ep" or "mc"\n)");
            return 1;
        }
    }

    testcases = get_tests();

    /* set up the suite if needed */
    harness.default_engine_cfg = engine_args;

    /* Check to see whether the config string string sets the bucket type. */
    if (harness.default_engine_cfg != nullptr) {
        std::regex bucket_type("bucket_type=(\\w+)",
                               std::regex_constants::ECMAScript);
        std::cmatch matches;
        if (std::regex_search(
                    harness.default_engine_cfg, matches, bucket_type)) {
            harness.bucket_type = matches.str(1);
        }
    }

    const auto num_cases = testcases.size();

    if (!setup_suite(&harness)) {
        std::cerr << "Failed to set up test suite" << std::endl;
        return 1;
    }

    do {
        bool need_newline = false;
        for (int i = 0; i < gsl::narrow_cast<int>(num_cases); i++) {
            // If a specific test was chosen, skip all other tests.
            if (test_case_id != -1 && i != test_case_id) {
                continue;
            }

            int error = 0;
            if (test_case_regex && !std::regex_search(testcases[i].name,
                                                      *test_case_regex)) {
                continue;
            }
            if (!quiet) {
                printf("Running [%04d/%04d]: %s...",
                       gsl::narrow_cast<int>(i + num_cases * loop_count),
                       gsl::narrow_cast<int>(num_cases * (loop_count + 1)),
                       testcases[i].name.c_str());
                fflush(stdout);
            } else if(dot) {
                printf(".");
                need_newline = true;
                /* Add a newline every few tests */
                if ((i+1) % 70 == 0) {
                    printf("\n");
                    need_newline = false;
                }
            }

            {
                enum test_result ecode = FAIL;

                for (int attempt = 0;
                     (attempt < attempts) && ((ecode != SUCCESS) &&
                                              (ecode != SUCCESS_AFTER_RETRY));
                     attempt++) {
                    auto start = std::chrono::steady_clock::now();
                    if (testcases[i].tfun || testcases[i].api_v2.tfun) {
                        // check there's a test to run, some modules need
                        // cleaning up of dead tests if all modules are fixed,
                        // this else if can be removed.
                        try {
                            ecode = execute_test(
                                    testcases[i], engine.c_str(), engine_args);
                        } catch (const TestExpectationFailed&) {
                            ecode = FAIL;
                        } catch (const std::exception& e) {
                            fprintf(stderr,
                                    "Uncaught std::exception. what():%s\n",
                                    e.what());
                            ecode = DIED;
                        } catch (...) {
                            // This is a non-test exception (i.e. not an
                            // explicit test check which failed) - mark as
                            // "died".
                            ecode = DIED;
                        }
                    } else {
                        ecode = PENDING; // ignored tests would always return
                                         // PENDING
                    }
                    auto stop = std::chrono::steady_clock::now();

                    /* If we only got SUCCESS after one or more
                       retries, change result to
                       SUCCESS_AFTER_RETRY */
                    if ((ecode == SUCCESS) && (attempt > 0)) {
                        ecode = SUCCESS_AFTER_RETRY;
                    }
                    error = report_test(testcases[i].name.c_str(),
                                        stop - start,
                                        ecode,
                                        quiet,
                                        !verbose);
                }
            }

            if (error != 0) {
                ++exitcode;
                if (terminate_on_error) {
                    exit(EXIT_FAILURE);
                }
            }
        }

        if (need_newline) {
            printf("\n");
        }
        ++loop_count;
    } while (loop && exitcode == 0);

    // tear down the suite if needed
    if (!teardown_suite()) {
        std::cerr << "Failed to teardown test suite" << std::endl;
    }

    printf("# Passed %d of %d tests\n",
           gsl::narrow_cast<int>(num_cases - exitcode),
           gsl::narrow_cast<int>(num_cases));

    return exitcode;
}
