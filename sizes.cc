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
    return 0;
}
