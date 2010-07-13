/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
#include "config.h"
#include "common.hh"
#undef NDEBUG
#include <assert.h>

int main(int argc, char **argv) {
   (void)argc; (void)argv;

   assert(hrtime2text(0) == "0 usec");
   assert(hrtime2text(9999) == "9999 usec");
   assert(hrtime2text(10000) == "10 ms");
   assert(hrtime2text(9999999) == "9999 ms");
   assert(hrtime2text(10000000) == "10 s");
   assert(hrtime2text(9999999999) == "9999 s");
   assert(hrtime2text(10000000000) == "10000 s");

   hrtime_t now = gethrtime();
   usleep(200);
   hrtime_t later = gethrtime();
   assert(now + 200 < later);

   return 0;
}
