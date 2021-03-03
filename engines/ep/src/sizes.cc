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

#include "checkpoint.h"
#include "checkpoint_config.h"
#include "checkpoint_manager.h"
#include "collections/vbucket_manifest_entry.h"
#include "common.h"
#include "couch-kvstore/couch-kvstore.h"
#include "dcp/response.h"
#include "hash_table_stat_visitor.h"
#include "item.h"
#include "kvstore_priv.h"
#ifdef EP_USE_MAGMA
#include "magma-kvstore/magma-kvstore_iorequest.h"
#include "magma-kvstore/magma-kvstore_metadata.h"
#endif
#include "persistence_callback.h"
#include "probabilistic_counter.h"
#include "stats.h"
#include "stored-value.h"
#include "vbucket.h"
#include "vbucketmap.h"
#include <platform/histogram.h>
#include <spdlog/fmt/fmt.h>

template <typename T>
static void display(const char* name, T size) {
    fmt::print("{:<40} {:>12} {:>12}\n", name, size, "-");
}

template <typename T, typename U>
static void display(const char* name, T size, U memFootprint) {
    fmt::print("{:<40} {:>12} {:>12}\n", name, size, memFootprint);
}

static void ruler() {
    fmt::print("{:-<40} {:->12} {:->12}\n", "", "", "");
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

    EPStats stats;
    const size_t size = GlobalTask::allTaskIds.size();
    stats.schedulingHisto.resize(size);
    stats.taskRuntimeHisto.resize(size);

    display("Type", "Sizeof (B)", "Mem Footprint (B)");
    ruler();

    display("Blob", sizeof(Blob));
    display("CheckpointManager", sizeof(CheckpointManager));
    display("Checkpoint", sizeof(Checkpoint));
    display("CheckpointConfig", sizeof(CheckpointConfig));
    display("Collections::VB::ManifestEntry",
            sizeof(Collections::VB::ManifestEntry));
    display("CouchRequest", sizeof(CouchRequest));
    display("DcpResponse", sizeof(DcpResponse));
    display("EPStats", sizeof(EPStats), stats.getMemFootPrint());
    display("FileStats", sizeof(FileStats), FileStats().getMemFootPrint());
    display("Item", sizeof(Item));
    display("HashTable", sizeof(HashTable));
    display("HdrHistogram frequency histo",
            sizeof(stats.activeOrPendingFrequencyValuesEvictedHisto),
            stats.activeOrPendingFrequencyValuesEvictedHisto.getMemFootPrint());
    display("Hdr1sfMicroSecHistogram",
            sizeof(Hdr1sfMicroSecHistogram),
            Hdr1sfMicroSecHistogram().getMemFootPrint());
    display("Hdr1sfInt32Histogram",
            "-",
            Hdr1sfInt32Histogram().getMemFootPrint());
    display("Hdr2sfMicroSecHistogram",
            "-",
            Hdr2sfMicroSecHistogram().getMemFootPrint());
    display("HdrHistogram(1, <int32_t>::max(), 2)",
            "-",
            HdrHistogram(1, std::numeric_limits<int32_t>::max() - 1, 2)
                    .getMemFootPrint());
    display("HdrHistogram(1, <int32_t>::max(), 1)",
            "-",
            HdrHistogram(1, std::numeric_limits<int32_t>::max() - 1, 1)
                    .getMemFootPrint());
    display("HdrHistogram(1, <int64_t>::max(), 2)",
            "-",
            HdrHistogram(1, std::numeric_limits<int64_t>::max() - 1, 2)
                    .getMemFootPrint());
    display("HdrHistogram(1, <int64_t>::max(), 1)",
            "-",
            HdrHistogram(1, std::numeric_limits<int64_t>::max() - 1, 1)
                    .getMemFootPrint());
    display("HdrUint8Histogram", "-", HdrUint8Histogram().getMemFootPrint());
    display("Histogram<whatever>", "-", Histogram<size_t>().getMemFootPrint());
    display("HistogramBin<size_t>", sizeof(HistogramBin<size_t>));
    display("HistogramBin<int>", sizeof(HistogramBin<int>));
    display("HistogramBin<microseconds>",
            sizeof(MicrosecondHistogram::bin_type));
    display("IORequest", sizeof(IORequest));
    display("KVStoreStats", "-", KVStoreStats().getMemFootPrint());
#ifdef EP_USE_MAGMA
    display("MagmaRequest", sizeof(MagmaRequest));
    display("magmakv::MetaData", sizeof(magmakv::MetaData));
#endif
    display("MicrosecondHistogram",
            "-",
            MicrosecondHistogram().getMemFootPrint());
    display("MutationResponse", sizeof(MutationResponse));
    display("OrderedStoredValue (with 1 byte key)",
            sizeof(OrderedStoredValue),
            OrderedStoredValue::getRequiredStorage(
                    DocKey("1", DocKeyEncodesCollectionId::No)));
    display("OrderedStoredValue (with 15 byte key)",
            sizeof(OrderedStoredValue),
            OrderedStoredValue::getRequiredStorage(
                    DocKey("1234567890abcde", DocKeyEncodesCollectionId::No)));
    display("PersistenceCallback", sizeof(PersistenceCallback));
    display("ProbabilisticCounter<uint8_t>",
            sizeof(ProbabilisticCounter<uint8_t>));
    display("queued_item", sizeof(queued_item));
    display("StoredValue (with 1 byte key)",
            sizeof(StoredValue),
            StoredValue::getRequiredStorage(
                    DocKey("1", DocKeyEncodesCollectionId::No)));
    display("StoredValue (with 15 byte key)",
            sizeof(StoredValue),
            StoredValue::getRequiredStorage(
                    DocKey("1234567890abcde", DocKeyEncodesCollectionId::No)));
    display("VBucket", sizeof(VBucket));
    display("VBucketMap", sizeof(VBucketMap));
    display("value_t", sizeof(value_t));
}
