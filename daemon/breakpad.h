/* -*- Mode: C; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2014 Couchbase, Inc
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
#ifndef BREAKPAD_H
#define BREAKPAD_H

/* Execute code in a C abstraction layer. */
#ifdef __cplusplus
extern "C" {
#endif

/* Initialize breakpad (mechanism for catching & reporting crashes).
 * Exception handler creates a dump file (at /tmp)
 * in the event of a fatal signal. (Assuming Breakpad is enabled)
 */
void initialize_breakpad(void);

/* Cleaning up when breakpad no longer needed
 * (Assuming it is enabled and has been initialized)
 */
void destroy_breakpad(void);

#ifdef __cplusplus
}
#endif

#endif /* BREAKPAD_H */
