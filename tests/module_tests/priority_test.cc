/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */

#include "config.h"

#include <assert.h>
#include "priority.h"
#undef NDEBUG

int main(int argc, char **argv) {
   (void)argc; (void)argv;

   assert(Priority::BgFetcherPriority > Priority::TapBgFetcherPriority);
   assert(Priority::TapBgFetcherPriority == Priority::VBucketDeletionPriority);
   assert(Priority::VBucketPersistHighPriority > Priority::VKeyStatBgFetcherPriority);
   assert(Priority::VKeyStatBgFetcherPriority > Priority::FlusherPriority);
   assert(Priority::FlusherPriority > Priority::ItemPagerPriority);
   assert(Priority::ItemPagerPriority > Priority::BackfillTaskPriority);

   return 0;
}
