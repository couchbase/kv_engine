/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2016 Couchbase, Inc.
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
extern "C" int memcached_main(int argc, char** argv);

/**
 * This is the program entry point for memcached. We're building the
 * memcached source as a static archive so that we may link it into other
 * programs (like memcached_testapp) and run it inside the process. The
 * main motivation for doing so is to make the life a lot easier when
 * trying to debug a unit test case (so that you can set breakpoints in
 * the same IDE rather than having to add spin locks and attach another
 * debugger to the memcached process spawned by the test case etc)
 *
 * @param argc argument count
 * @param argv argument vector
 * @return the process exit number
 */
int main(int argc, char** argv) {
    return memcached_main(argc, argv);
}
