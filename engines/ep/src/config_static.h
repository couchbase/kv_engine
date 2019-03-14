/* -*- Mode: C; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
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

/* Consider this file as an extension to config.h, just that it contains
 * static text. The intention is to reduce the number of #ifdefs in the rest
 * of the source files without having to put all of them in AH_BOTTOM
 * in configure.ac.
 */

#pragma once

#include "config.h"

#define _FILE_OFFSET_BITS 64

#if HAVE_ARPA_INET_H
#include <arpa/inet.h>
#endif

#ifdef WIN32
#include <winsock2.h>
#include <ws2tcpip.h>

#include <io.h>
#define F_OK 0
#define W_OK 2
#define R_OK 4
#pragma warning(disable: 4291)
#pragma warning(disable: 4244)
#pragma warning(disable: 4267)
#pragma warning(disable: 4996)

typedef unsigned int useconds_t;
#endif // WIN32

#if defined(linux) || defined(__linux__) || defined(__linux)
#undef ntohs
#undef ntohl
#undef htons
#undef htonl
#endif
