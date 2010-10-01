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

#include "item.hh"
#include "stored-value.hh"
#include "ep.hh"
#include "vbucket.hh"

static void display(const char *name, size_t size) {
    std::cout << name << "\t" << size << std::endl;
}

int main(int argc, char **argv) {
    (void)argc; (void)argv;

    std::string s();

    display("GIGANTOR", GIGANTOR);
    display("Small Stored Value", StoredValue::sizeOf(true));
    display("Featured Stored Value", StoredValue::sizeOf(false));

    display("... Small data", sizeof(struct small_data));
    display("... Feature data", sizeof(struct feature_data));
    display("... Bodies Union", sizeof(union stored_value_bodies));

    display("Stored Value Factory", sizeof(StoredValueFactory));
    display("Blob", sizeof(Blob));
    display("value_t", sizeof(value_t));
    display("HashTable", sizeof(HashTable));
    display("Item", sizeof(Item));
    display("QueuedItem", sizeof(QueuedItem));
    display("VBucket", sizeof(VBucket));
    display("VBucketHolder", sizeof(VBucketHolder));
    display("VBucketMap", sizeof(VBucketMap));
    display("Stats", sizeof(EPStats));
    display("Histogram<whatever>", sizeof(Histogram<size_t>));
    display("HistogramBin<size_t>", sizeof(HistogramBin<size_t>));
    display("HistogramBin<hrtime_t>", sizeof(HistogramBin<hrtime_t>));
    display("HistogramBin<int>", sizeof(HistogramBin<int>));
    return 0;
}
