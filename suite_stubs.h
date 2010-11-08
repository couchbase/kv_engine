#ifndef TESTSUITE_H
#define TESTSUITE_H 1

#include <assert.h>

#include "ep_testsuite.h"

bool teardown(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1);
void delay(int amt);

void add(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1);
void append(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1);
void decr(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1);
void decrWithDefault(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1);
void prepend(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1);
void flush(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1);
void del(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1);
void set(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1);
void incr(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1);
void incrWithDefault(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1);

void checkValue(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1, const char* exp);
void assertNotExists(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1);

#define assertHasError() assert(hasError)
#define assertHasNoError() assert(!hasError)

extern int expiry;
extern bool hasError;
extern struct test_harness testHarness;

#endif /* TESTSUITE_H */
