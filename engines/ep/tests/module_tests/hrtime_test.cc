/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2010 Couchbase, Inc
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

#include "common.h"
#undef NDEBUG

int main(void)
{
   hrtime_t val = 0;
   cb_assert(hrtime2text(val) == std::string("0 ns"));
   val = 9;
   cb_assert(hrtime2text(val) == std::string("9 ns"));
   val = 99;
   cb_assert(hrtime2text(val) == std::string("99 ns"));
   val = 999;
   cb_assert(hrtime2text(val) == std::string("999 ns"));
   val = 9999;
   cb_assert(hrtime2text(val) == std::string("9999 ns"));
   val *= 1000;
   cb_assert(hrtime2text(val) == std::string("9999 usec"));
   val *= 1000;
   cb_assert(hrtime2text(val) == std::string("9999 ms"));
   val = 599;
   val *= 1000*1000*1000;
   cb_assert(hrtime2text(val) == std::string("599 s"));
   val = 600;
   val *= 1000*1000*1000;
   cb_assert(hrtime2text(val) == std::string("0h:10m:0s"));
   val = 1;
   cb_assert(hrtime2text(val) == std::string("1 ns"));
   val = 10;
   cb_assert(hrtime2text(val) == std::string("10 ns"));
   val = 100;
   cb_assert(hrtime2text(val) == std::string("100 ns"));
   val = 1000;
   cb_assert(hrtime2text(val) == std::string("1000 ns"));
   val = 10000;
   cb_assert(hrtime2text(val) == std::string("10 usec"));
   val *= 1000;
   cb_assert(hrtime2text(val) == std::string("10 ms"));
   val *= 1000;
   cb_assert(hrtime2text(val) == std::string("10 s"));

   hrtime_t now = gethrtime();
   usleep(200);
   hrtime_t later = gethrtime();
   cb_assert(now + 200 < later);

   return 0;
}
