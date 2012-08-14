/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
#include "config.h"
#include "priority.hh"
#undef NDEBUG
#include <assert.h>

int main(int argc, char **argv) {
   (void)argc; (void)argv;

   assert(Priority::BgFetcherPriority > Priority::TapBgFetcherPriority);
   assert(Priority::TapBgFetcherPriority == Priority::VBucketPersistHighPriority);
   assert(Priority::VBucketPersistHighPriority > Priority::VKeyStatBgFetcherPriority);
   assert(Priority::VKeyStatBgFetcherPriority > Priority::FlusherPriority);
   assert(Priority::FlusherPriority > Priority::ItemPagerPriority);
   assert(Priority::ItemPagerPriority > Priority::BackfillTaskPriority);

   return 0;
}
