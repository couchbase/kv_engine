/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2016-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */

#include "file_ops_tracker.h"
#include "kvstore/couch-kvstore/couch-fs-stats.h"
#include "kvstore/kvstore.h"
#include "programs/engine_testapp/mock_server.h"
#include "tests/wrapped_fileops_test.h"
#include <folly/portability/GTest.h>

class TestStatsOps : public StatsOps {
public:
    explicit TestStatsOps(FileOpsInterface* ops)
        : StatsOps(_stats, FileOpsTracker::instance(), *ops), wrapped_ops(ops) {
    }

    couch_file_handle constructor(couchstore_error_info_t *errinfo) override {
            FileOpsInterface* orig_ops = wrapped_ops.get();
            auto* sf = new StatFile(orig_ops,
                                        orig_ops->constructor(errinfo),
                                        0);
        return reinterpret_cast<couch_file_handle>(sf);
    }
protected:
    FileStats _stats;
    std::unique_ptr<FileOpsInterface> wrapped_ops;
};

int main() {
    initialize_time_functions(get_mock_server_api()->core);
    return RUN_ALL_TESTS();
}

typedef testing::Types<TestStatsOps>
    WrappedOpsImplementation;

INSTANTIATE_TYPED_TEST_SUITE_P(CouchstoreOpsTest,
                               WrappedOpsTest,
                               WrappedOpsImplementation);

INSTANTIATE_TYPED_TEST_SUITE_P(CouchstoreOpsTest,
                               UnbufferedWrappedOpsTest,
                               WrappedOpsImplementation);

GTEST_ALLOW_UNINSTANTIATED_PARAMETERIZED_TEST(BufferedWrappedOpsTest);
