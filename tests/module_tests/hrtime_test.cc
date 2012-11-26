/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */

#include "config.h"

#include <assert.h>
#include "common.h"
#undef NDEBUG

int main(void)
{
   hrtime_t val = 0;
   assert(hrtime2text(val) == std::string("0 ns"));
   val = 9;
   assert(hrtime2text(val) == std::string("9 ns"));
   val = 99;
   assert(hrtime2text(val) == std::string("99 ns"));
   val = 999;
   assert(hrtime2text(val) == std::string("999 ns"));
   val = 9999;
   assert(hrtime2text(val) == std::string("9999 ns"));
   val *= 1000;
   assert(hrtime2text(val) == std::string("9999 usec"));
   val *= 1000;
   assert(hrtime2text(val) == std::string("9999 ms"));
   val = 599;
   val *= 1000*1000*1000;
   assert(hrtime2text(val) == std::string("599 s"));
   val = 600;
   val *= 1000*1000*1000;
   assert(hrtime2text(val) == std::string("0h:10m:0s"));
   val = 1;
   assert(hrtime2text(val) == std::string("1 ns"));
   val = 10;
   assert(hrtime2text(val) == std::string("10 ns"));
   val = 100;
   assert(hrtime2text(val) == std::string("100 ns"));
   val = 1000;
   assert(hrtime2text(val) == std::string("1000 ns"));
   val = 10000;
   assert(hrtime2text(val) == std::string("10 usec"));
   val *= 1000;
   assert(hrtime2text(val) == std::string("10 ms"));
   val *= 1000;
   assert(hrtime2text(val) == std::string("10 s"));

   hrtime_t now = gethrtime();
   usleep(200);
   hrtime_t later = gethrtime();
   assert(now + 200 < later);

   return 0;
}
