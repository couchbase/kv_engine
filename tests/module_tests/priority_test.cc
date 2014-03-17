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

#include "priority.h"
#undef NDEBUG

int main(int argc, char **argv) {
   (void)argc; (void)argv;

   cb_assert(Priority::BgFetcherPriority > Priority::TapBgFetcherPriority);
   cb_assert(Priority::TapBgFetcherPriority == Priority::VBucketDeletionPriority);
   cb_assert(Priority::VBucketPersistHighPriority > Priority::VKeyStatBgFetcherPriority);
   cb_assert(Priority::VKeyStatBgFetcherPriority > Priority::FlusherPriority);
   cb_assert(Priority::FlusherPriority > Priority::ItemPagerPriority);
   cb_assert(Priority::ItemPagerPriority > Priority::BackfillTaskPriority);

   return 0;
}
