/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
#include "config.h"
#include "priority.hh"
#undef NDEBUG
#include <assert.h>

int main(int argc, char **argv) {
   (void)argc; (void)argv;

   assert(Priority::Low < Priority::High);
   assert(!(Priority::High < Priority::Low));
   assert(Priority::High > Priority::Low);
   assert(!(Priority::Low > Priority::High));
   assert(!(Priority::Low == Priority::High));
   assert(!(Priority::High == Priority::Low));
   assert(Priority::Low == Priority::Low);
   assert(Priority::High == Priority::High);

   return 0;
}
