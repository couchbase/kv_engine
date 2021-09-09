/*
 *     Copyright 2010-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */
#include <stdio.h>

#include <algorithm>
#include <limits>

#include "checkpoint.h"
#include "checkpoint_config.h"
#include "checkpoint_manager.h"
#include "collections/vbucket_manifest_entry.h"
#include "common.h"
#include "dcp/active_stream.h"
#include "dcp/consumer.h"
#include "dcp/passive_stream.h"
#include "dcp/producer.h"
#include "dcp/response.h"
#include "dcp/stream_container.h"
#include "hash_table_stat_visitor.h"
#include "item.h"
#include "kvstore/couch-kvstore/couch-kvstore.h"
#include "kvstore/kvstore_priv.h"
#ifdef EP_USE_MAGMA
#include "kvstore/magma-kvstore/kv_magma_common/magma-kvstore_metadata.h"
#include "kvstore/magma-kvstore/magma-kvstore_iorequest.h"
#endif
#include "kvstore/persistence_callback.h"
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
    display("DCP", "", "");
    display("    ActiveStream", sizeof(ActiveStream));
    display("    DcpConsumer", sizeof(DcpConsumer));
    display("    DcpProducer", sizeof(DcpProducer));
    display("    DcpResponse", sizeof(DcpResponse));
    display("    PassiveStream", sizeof(PassiveStream));
    display("    StreamContainer<ContainerElement>",
            sizeof(StreamContainer<DcpProducer::ContainerElement>));
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
