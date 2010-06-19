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
    display("Featured Stored Value", sizeof(FeaturedStoredValue));
    display("HashTable", sizeof(HashTable));
    display("Item", sizeof(Item));
    display("QueuedItem", sizeof(QueuedItem));
    display("VBucket", sizeof(VBucket));
    display("VBucketHolder", sizeof(VBucketHolder));
    display("VBucketMap", sizeof(VBucketMap));
    return 0;
}
