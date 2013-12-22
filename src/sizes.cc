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

#include "config.h"

#include <stdio.h>

#include <algorithm>
#include <limits>

#include "checkpoint.h"
#include "ep.h"
#include "histo.h"
#include "item.h"
#include "stored-value.h"
#include "vbucket.h"

static void display(const char *name, size_t size) {
    std::cout << name << "\t" << size << std::endl;
}

template <typename T>
struct histo_for_inner {
    void operator()(const HistogramBin<T> *bin) {
        std::cout << "   " << bin->start() << " - ";
        if (bin->end() == std::numeric_limits<T>::max()) {
            std::cout << "inf";
        } else {
            std::cout << bin->end();
        }
        std::cout << std::endl;
    }
};

template <>
struct histo_for_inner<hrtime_t> {
    void operator()(const HistogramBin<hrtime_t> *bin) {
        const std::string endtext(bin->end() == std::numeric_limits<hrtime_t>::max()
                                  ? "inf"
                                  : hrtime2text(bin->end()));
        std::cout << "   " << hrtime2text(bin->start())
                  << " - " << endtext << std::endl;
    }
};

template <typename T>
static void display(const char *name, const Histogram<T> &histo) {
    std::cout << name << std::endl;
    std::for_each(histo.begin(), histo.end(), histo_for_inner<T>());
}

int main(int, char **) {
    std::string s;

    display("GIGANTOR", GIGANTOR);
    display("Stored Value", sizeof(StoredValue));

    display("Stored Value Factory", sizeof(StoredValueFactory));
    display("Blob", sizeof(Blob));
    display("value_t", sizeof(value_t));
    display("HashTable", sizeof(HashTable));
    display("Item", sizeof(Item));
    display("QueuedItem", sizeof(QueuedItem));
    display("VBucket", sizeof(VBucket));
    display("VBucketMap", sizeof(VBucketMap));
    display("Stats", sizeof(EPStats));
    display("CheckpointManager", sizeof(CheckpointManager));
    display("Checkpoint\t", sizeof(Checkpoint));
    display("CheckpointConfig", sizeof(CheckpointConfig));
    display("Histogram<whatever>", sizeof(Histogram<size_t>));
    display("HistogramBin<size_t>", sizeof(HistogramBin<size_t>));
    display("HistogramBin<hrtime_t>", sizeof(HistogramBin<hrtime_t>));
    display("HistogramBin<int>", sizeof(HistogramBin<int>));

    std::cout << std::endl << "Histogram Ranges" << std::endl << std::endl;

    EPStats stats;
    HashTableDepthStatVisitor dv;
    display("Default Histo", stats.diskInsertHisto);
    display("Commit Histo", stats.diskCommitHisto);
    display("Hash table depth histo", dv.depthHisto);
    return 0;
}
