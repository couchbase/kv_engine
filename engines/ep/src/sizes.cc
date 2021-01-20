/*
 *     Copyright 2010 NorthScale, Inc.
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
#include <stdio.h>

#include <algorithm>
#include <limits>

#include "atomic_unordered_map.h"
#include "checkpoint.h"
#include "checkpoint_config.h"
#include "checkpoint_manager.h"
#include "collections/vbucket_manifest_entry.h"
#include "common.h"
#include "couch-kvstore/couch-kvstore.h"
#include "dcp/response.h"
#include "dcp/stream.h"
#include "hash_table_stat_visitor.h"
#include "item.h"
#include "kvstore_priv.h"
#include "persistence_callback.h"
#include "probabilistic_counter.h"
#include "stats.h"
#include "stored-value.h"
#include "vbucket.h"
#include "vbucketmap.h"
#include <platform/histogram.h>
#include <platform/timeutils.h>

static void display(const char *name, size_t size) {
    std::cout << name << "\t" << size << std::endl;
}

template <typename T, template <class> class Traits>
struct histo_for_inner {
    void operator()(const std::unique_ptr<HistogramBin<T, Traits>>& bin) {
        std::cout << "   " << bin->start() << " - ";
        if (bin->end() == Traits<T>::max()) {
            std::cout << "inf";
        } else {
            std::cout << bin->end();
        }
        std::cout << std::endl;
    }
};

template <typename T, template <class> class Traits>
static void display(const char* name, const Histogram<T, Traits>& histo) {
    std::cout << name << std::endl;
    std::for_each(histo.begin(), histo.end(), histo_for_inner<T, Traits>());
}

int main(int, char **) {
    std::string s;

    display("GIGANTOR", GIGANTOR);
    display("StoredValue", sizeof(StoredValue));
    display("StoredValue with 15 byte key",
            StoredValue::getRequiredStorage(
                    DocKey("1234567890abcde", DocKeyEncodesCollectionId::No)));
    display("Ordered Stored Value", sizeof(OrderedStoredValue));
    display("Blob", sizeof(Blob));
    display("value_t", sizeof(value_t));
    display("HashTable", sizeof(HashTable));
    display("Item", sizeof(Item));
    display("VBucket", sizeof(VBucket));
    display("VBucketMap", sizeof(VBucketMap));
    display("Stats", sizeof(EPStats));
    display("CheckpointManager", sizeof(CheckpointManager));
    display("Checkpoint\t", sizeof(Checkpoint));
    display("CheckpointConfig", sizeof(CheckpointConfig));
    display("Histogram<whatever>", Histogram<size_t>().getMemFootPrint());
    display("HistogramBin<size_t>", sizeof(HistogramBin<size_t>));
    display("HistogramBin<int>", sizeof(HistogramBin<int>));
    display("HistogramBin<microseconds>",
            sizeof(MicrosecondHistogram::bin_type));
    display("MicrosecondHistogram", MicrosecondHistogram().getMemFootPrint());
    EPStats stats;
    const size_t size = GlobalTask::allTaskIds.size();
    stats.schedulingHisto.resize(size);
    stats.taskRuntimeHisto.resize(size);
    display("EPStats", stats.getMemFootPrint());
    display("FileStats", FileStats().getMemFootPrint());
    display("KVStoreStats", KVStoreStats().getMemFootPrint());
    display("Histogram<size_t>{ExponentialGenerator<size_t>(1, 2), 50}",
            Histogram<size_t>{ExponentialGenerator<size_t>(1, 2), 50}
                    .getMemFootPrint());
    display("HdrHistogram frequency histo",
            stats.activeOrPendingFrequencyValuesEvictedHisto.getMemFootPrint());
    display("Hdr1sfMicroSecHistogram",
            Hdr1sfMicroSecHistogram().getMemFootPrint());
    display("Hdr2sfMicroSecHistogram",
            Hdr2sfMicroSecHistogram().getMemFootPrint());

    display("HdrUint8Histogram", HdrUint8Histogram().getMemFootPrint());
    display("Hdr1sfInt32Histogram", Hdr1sfInt32Histogram().getMemFootPrint());

    display("HdrHistogram(0, std::numeric_limits<int32_t>::max(), 2)",
            HdrHistogram(0, std::numeric_limits<int32_t>::max() - 1, 2)
                    .getMemFootPrint());
    display("HdrHistogram(0, std::numeric_limits<int32_t>::max(), 1)",
            HdrHistogram(0, std::numeric_limits<int32_t>::max() - 1, 1)
                    .getMemFootPrint());
    display("HdrHistogram(0, std::numeric_limits<int64_t>::max(), 2)",
            HdrHistogram(0, std::numeric_limits<int64_t>::max() - 1, 2)
                    .getMemFootPrint());
    display("HdrHistogram(0, std::numeric_limits<int64_t>::max(), 1)",
            HdrHistogram(0, std::numeric_limits<int64_t>::max() - 1, 1)
                    .getMemFootPrint());

    display("IORequest", sizeof(IORequest));
    display("CouchRequest", sizeof(CouchRequest));
    display("PersistenceCallback", sizeof(PersistenceCallback));
    display("AtomicUnorderedMap<uint32_t, SingleThreadedRCPtr<Stream>>",
            sizeof(AtomicUnorderedMap<uint32_t, SingleThreadedRCPtr<Stream>>));
    display("ProbabilisticCounter<uint8_t>",
            sizeof(ProbabilisticCounter<uint8_t>));
    display("DcpResponse", sizeof(DcpResponse));
    display("MutationResponse", sizeof(MutationResponse));
    display("queued_item", sizeof(queued_item));
    display("Collections::VB::ManifestEntry",
            sizeof(Collections::VB::ManifestEntry));
    display("cb::ExpiryLimit", sizeof(cb::ExpiryLimit));

    std::cout << std::endl << "Histogram Ranges" << std::endl << std::endl;

    HashTableDepthStatVisitor dv;
    display("Default Histo", stats.diskInsertHisto.getMemFootPrint());
    display("Commit Histo", stats.diskCommitHisto.getMemFootPrint());
    display("Hash table depth histo", dv.depthHisto.getMemFootPrint());
    return 0;
}
