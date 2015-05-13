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

#include "ep_testsuite_common.h"
#include "ep_test_apis.h"

#include <cstring>
#include <iostream>
#include <string>
#include <sstream>

#include <sys/stat.h>
#ifdef _MSC_VER
#include <direct.h>
#define mkdir(a, b) _mkdir(a)
#else
#include <sys/wait.h>
#endif

#include <platform/dirutils.h>

static enum test_result skipped_test_function(ENGINE_HANDLE *h,
                                              ENGINE_HANDLE_V1 *h1);

TestCase::TestCase(const char *_name,
                   enum test_result(*_tfun)(ENGINE_HANDLE *, ENGINE_HANDLE_V1 *),
                   bool(*_test_setup)(ENGINE_HANDLE *, ENGINE_HANDLE_V1 *),
                   bool(*_test_teardown)(ENGINE_HANDLE *, ENGINE_HANDLE_V1 *),
                   const char *_cfg,
                   enum test_result (*_prepare)(engine_test_t *test),
                   void (*_cleanup)(engine_test_t *test, enum test_result result),
                   bool _skip)
  : name(_name),
    cfg(_cfg),
    skip(_skip) {

    memset(&test, 0, sizeof(test));
    test.tfun = _tfun;
    test.test_setup = _test_setup;
    test.test_teardown = _test_teardown;
    test.prepare = _prepare;
    test.cleanup = _cleanup;
}

TestCase::TestCase(const TestCase &o)
  : name(o.name),
    cfg(o.cfg),
    skip(o.skip) {

    memset(&test, 0, sizeof(test));
    test.tfun = o.test.tfun;
    test.test_setup = o.test.test_setup;
    test.test_teardown = o.test.test_teardown;
}

engine_test_t* TestCase::getTest() {
    engine_test_t *ret = &test;

    std::string nm(name);
    std::stringstream ss;

    if (cfg != 0) {
        ss << cfg << ";";
    } else {
        ss << "flushall_enabled=true;";
    }

    if (skip) {
        nm.append(" (skipped)");
        ret->tfun = skipped_test_function;
    } else {
        nm.append(" (couchstore)");
    }

    ret->name = strdup(nm.c_str());
    std::string config = ss.str();
    if (config.length() == 0) {
        ret->cfg = 0;
    } else {
        ret->cfg = strdup(config.c_str());
    }

    return ret;
}

static enum test_result skipped_test_function(ENGINE_HANDLE *h,
                                              ENGINE_HANDLE_V1 *h1) {
    (void) h;
    (void) h1;
    return SKIPPED;
}

const char *dbname_env;
enum test_result rmdb(void) {
    const char *files[] = { WHITESPACE_DB,
                            "/tmp/test",
                            "/tmp/mutation.log",
                            dbname_env,
                            NULL };
    int ii = 0;
    while (files[ii] != NULL) {
        CouchbaseDirectoryUtilities::rmrf(files[ii]);
        if (access(files[ii], F_OK) != -1) {
            std::cerr << "Failed to remove: " << files[ii] << " " << std::endl;
            return FAIL;
        }
        ++ii;
    }

    return SUCCESS;
}

bool test_setup(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1) {
    wait_for_warmup_complete(h, h1);

    check(h1->get_stats(h, NULL, "prev-vbucket", 12, add_stats) == ENGINE_SUCCESS,
          "Failed to get the previous state of vbuckets");
    if (vals.find("vb_0") == vals.end()) {
        check(set_vbucket_state(h, h1, 0, vbucket_state_active),
              "Failed to set VB0 state.");
    }

    wait_for_stat_change(h, h1, "ep_vb_snapshot_total", 0);

    // warmup is complete, notify ep engine that it must now enable
    // data traffic
    protocol_binary_request_header *pkt = createPacket(PROTOCOL_BINARY_CMD_ENABLE_TRAFFIC);
    check(h1->unknown_command(h, NULL, pkt, add_response) == ENGINE_SUCCESS,
          "Failed to enable data traffic");
    free(pkt);

    return true;
}

bool teardown(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1) {
    (void)h; (void)h1;
    vals.clear();
    return true;
}

enum test_result prepare(engine_test_t *test) {
#ifdef __sun
        // Some of the tests doesn't work on Solaris.. Don't know why yet..
        if (strstr(test->name, "concurrent set") != NULL ||
            strstr(test->name, "retain rowid over a soft delete") != NULL)
        {
            return SKIPPED;
        }
#endif


    if (test->cfg == NULL || strstr(test->cfg, "backend") == NULL) {
        return rmdb();
    }

    enum test_result ret = rmdb();
    if (ret != SUCCESS) {
        return ret;
    }

    if (strstr(test->cfg, "backend=couchdb") != NULL) {
        std::string dbname;
        const char *nm = strstr(test->cfg, "dbname=");
        if (nm == NULL) {
            dbname.assign(dbname_env);
        } else {
            dbname.assign(nm + 7);
            std::string::size_type end = dbname.find(';');
            if (end != dbname.npos) {
                dbname = dbname.substr(0, end);
            }
        }
        if (dbname.find("/non/") == dbname.npos) {
            mkdir(dbname.c_str(), 0777);
        }
    } else {
        // unknow backend!
        using namespace std;

        cerr << endl << "Unknown backend specified! " << endl
             << test->cfg << endl;

        return FAIL;
    }
    return SUCCESS;
}

void cleanup(engine_test_t *test, enum test_result result) {
    (void)result;
    // Nuke the database files we created
    rmdb();
    free(const_cast<char*>(test->name));
    free(const_cast<char*>(test->cfg));
}

// Array of testcases to return back to engine_testapp.
static engine_test_t *testcases;

// Should only one test be run, and if so which number? If -1 then all tests
// are run.
static int oneTestIdx;

struct test_harness testHarness;

extern TestCase testsuite_testcases[];

// Examines the list of tests provided by the specific testsuite
// via the testsuite_testcases[] array, populates `testcases` and returns it.
MEMCACHED_PUBLIC_API
engine_test_t* get_tests(void) {

    // Calculate the size of the tests..
    int num = 0;
    while (testsuite_testcases[num].getName() != NULL) {
        ++num;
    }

    oneTestIdx = -1;
    char *testNum = getenv("EP_TEST_NUM");
    if (testNum) {
       sscanf(testNum, "%d", &oneTestIdx);
       if (oneTestIdx < 0 || oneTestIdx > num) {
           oneTestIdx = -1;
       }
    }
    dbname_env = getenv("EP_TEST_DIR");
    if (!dbname_env) {
        dbname_env = "/tmp/test";
    }

    if (oneTestIdx == -1) {
        testcases = static_cast<engine_test_t*>(calloc(num + 1,
                    sizeof(engine_test_t)));

        int ii = 0;
        for (int jj = 0; jj < num; ++jj) {
            engine_test_t *r = testsuite_testcases[jj].getTest();
            if (r != 0) {
                testcases[ii++] = *r;
            }
        }
    } else {
        testcases = static_cast<engine_test_t*>(calloc(1 + 1,
                    sizeof(engine_test_t)));

        engine_test_t *r = testsuite_testcases[oneTestIdx].getTest();
        if (r != 0) {
            testcases[0] = *r;
        }
    }

    return testcases;
}

MEMCACHED_PUBLIC_API
    bool setup_suite(struct test_harness *th) {
    putenv(const_cast<char*>("EP-ENGINE-TESTSUITE=true"));
    testHarness = *th;
    return true;
}


MEMCACHED_PUBLIC_API
    bool teardown_suite() {
    free(testcases);
    testcases = NULL;
    return true;
}
