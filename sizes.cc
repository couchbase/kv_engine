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

#include "item.hh"
#include "stored-value.hh"
#include "ep.hh"
#include "vbucket.hh"

static void display(const char *name, size_t size) {
    printf("%s\t%d\n", name, (int)size);
}

int main(int argc, char **argv) {
    (void)argc; (void)argv;

    std::string s();
    display("Stored Value", sizeof(StoredValue));
    display("HashTable", sizeof(HashTable));
    display("Item", sizeof(Item));
    display("QueuedItem", sizeof(QueuedItem));
    display("VBucket", sizeof(VBucket));
    display("VBucketHolder", sizeof(VBucketHolder));
    display("VBucketMap", sizeof(VBucketMap));
    return 0;
}
