/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2016 Couchbase, Inc
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

#include "tests/wrapped_fileops_test.h"
#include "src/couch-kvstore/couch-fs-stats.h"

class TestStatsOps : public StatsOps {
public:
    TestStatsOps(FileOpsInterface* ops)
        : StatsOps(_stats),
          wrapped_ops(ops) {}

    couch_file_handle constructor(couchstore_error_info_t *errinfo) override {
            FileOpsInterface* orig_ops = wrapped_ops.get();
            StatFile* sf = new StatFile(orig_ops,
                                        orig_ops->constructor(errinfo),
                                        0);
        return reinterpret_cast<couch_file_handle>(sf);
    }
protected:
    CouchstoreStats _stats;
    std::unique_ptr<FileOpsInterface> wrapped_ops;
};

typedef testing::Types<TestStatsOps>
    WrappedOpsImplementation;

INSTANTIATE_TYPED_TEST_CASE_P(CouchstoreOpsTest,
    WrappedOpsTest,
    WrappedOpsImplementation
);

INSTANTIATE_TYPED_TEST_CASE_P(CouchstoreOpsTest,
    UnbufferedWrappedOpsTest,
    WrappedOpsImplementation
);