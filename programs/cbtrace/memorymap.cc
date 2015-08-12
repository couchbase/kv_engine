/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2015 Couchbase, Inc.
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
#include <cstdlib>
#include <cstring>
#include <cerrno>

#include <iostream>
#include <sstream>
#include <stdexcept>

#include "memorymap.h"

using namespace std;

#ifdef WIN32
#include <windows.h>

MemoryMappedFile::MemoryMappedFile() :
    root(NULL), size(0),
    handle(INVALID_HANDLE_VALUE),
    mapping(INVALID_HANDLE_VALUE)
{
    // empty
}

void MemoryMappedFile::create(const char *file) {
    handle = CreateFile(file,
                        GENERIC_READ | GENERIC_WRITE,
                        FILE_SHARE_READ | FILE_SHARE_WRITE,
                        NULL,
                        OPEN_EXISTING,
                        FILE_ATTRIBUTE_NORMAL | FILE_FLAG_RANDOM_ACCESS,
                        NULL);

    if (handle == INVALID_HANDLE_VALUE) {
        std::string ss = "Failed to open file: ";
        ss += file;
        throw std::runtime_error(ss);
    }

    DWORD fsize = ::GetFileSize(handle, NULL);
    if (fsize == INVALID_FILE_SIZE && GetLastError() != NO_ERROR) {
        CloseHandle(handle);
        throw std::runtime_error("Failed to get file size");
    }

    size = (size_t)fsize;
    mapping = CreateFileMapping(handle, NULL, PAGE_READONLY, 0, 0, NULL);
    if (mapping == NULL) {
        CloseHandle(handle);
        throw std::runtime_error("Failed to create file mapping");
    }

    root = MapViewOfFile(mapping, FILE_MAP_READ, 0, 0, 0);
    if (root == NULL) {
        CloseHandle(mapping);
        CloseHandle(handle);
        throw std::runtime_error("Failed to map file");
    }
}

MemoryMappedFile::~MemoryMappedFile() {
    if (root != NULL) {
        UnmapViewOfFile(root);
        CloseHandle(mapping);
        CloseHandle(handle);
    }
}

#else

#include <sys/stat.h>
#include <sys/mman.h>

MemoryMappedFile::MemoryMappedFile()
    : root(NULL),
      size(0),
      fp(NULL) {
    // empty
}

void MemoryMappedFile::create(const char* file) {
    struct stat st;
    if (stat(file, &st) == -1) {
        std::stringstream ss;
        ss << "Failed to get file information for \"" << file
        << "\": " << strerror(errno);
        throw std::runtime_error(ss.str());
    }

    size = st.st_size;
    if ((fp = fopen(file, "r")) == NULL) {
        std::stringstream ss;
        ss << "Failed to open \"" << file
        << "\": " << strerror(errno);
        throw std::runtime_error(ss.str());
    }

    root = mmap(NULL, size, PROT_READ, MAP_PRIVATE, fileno(fp), 0);
    if (root == NULL) {
        std::stringstream ss;
        ss << "Failed to memory map \"" << file
        << "\": " << strerror(errno);
        throw std::runtime_error(ss.str());
    }

    (void) posix_madvise(root, size, POSIX_MADV_SEQUENTIAL);
}

MemoryMappedFile::~MemoryMappedFile() {

    if (root != NULL) {
        (void) munmap(root, size);
        fclose(fp);
    }
}

#endif
