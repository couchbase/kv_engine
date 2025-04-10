/*
 *     Copyright 2025-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */

#pragma once

#include <folly/portability/GMock.h>
#include <libmagma/file.h>
#include <libmagma/status.h>

/**
 * GMock for magma::File.
 */
class MockMagmaFile : public magma::File {
public:
    /// Mock with no default behaviour.
    MockMagmaFile() = default;
    /// Creates a mock file which delegates to the wrapped instance.
    MockMagmaFile(std::unique_ptr<File> wrapped);

    MOCK_METHOD1(Open, magma::Status(int));
    MOCK_METHOD1(Size, magma::Status(size_t*));
    MOCK_METHOD1(LastModificationTime, magma::Status(time_t*));
    MOCK_METHOD4(Read, magma::Status(int64_t, size_t, char*, size_t*));
    MOCK_METHOD3(Write, magma::Status(int64_t, const char*, size_t));
    MOCK_METHOD0(Sync, magma::Status());
    MOCK_METHOD0(Close, magma::Status());
    MOCK_METHOD1(Destroy, magma::Status(magma::FileRemover));
    MOCK_METHOD1(Copy, magma::Status(const std::string&));
    MOCK_METHOD1(Mmap, magma::Status(char**));
    MOCK_METHOD0(Munmap, magma::Status());
    MOCK_METHOD1(SetAccessPattern, void(AccessPattern));
    MOCK_METHOD2(ReadAhead, void(int64_t, size_t));
    MOCK_METHOD3(CachedSize, magma::Status(size_t, size_t, size_t&));
    MOCK_METHOD0(GetBlockSize, size_t());
    MOCK_METHOD2(Preallocate, magma::Status(size_t, bool));
    MOCK_METHOD1(Truncate, magma::Status(size_t));
    MOCK_CONST_METHOD0(GetPath, std::string());
    MOCK_METHOD0(Lock, magma::Status());
    MOCK_METHOD0(TryLock, std::pair<magma::Status, bool>());
    MOCK_METHOD0(Unlock, magma::Status());

    /// Returns true if the file is WAL.
    bool isWal() const;
    /// Returns true if the file is an SSTable data file.
    bool isSSTable() const;
    /// Returns true if the file is part of the keyIndex.
    bool isKeyIndex() const;
    /// Returns true if the file is part of the localIndex.
    bool isLocalIndex() const;
    /// Returns true if the file is part of the seqIndex.
    bool isSeqIndex() const;

private:
    const std::unique_ptr<File> wrapped;

    void delegateToWrapped();
};

/**
 * GMock for magma::Directory.
 */
class MockMagmaDirectory : public magma::Directory {
public:
    /// Mock with no default behaviour.
    MockMagmaDirectory() = default;
    /// Creates a mock directory which delegates to the wrapped instance.
    MockMagmaDirectory(std::unique_ptr<Directory> wrapped);

    MOCK_METHOD1(Open, magma::Status(OpenMode));
    MOCK_METHOD0(Sync, magma::Status());

private:
    const std::unique_ptr<Directory> wrapped;

    void delegateToWrapped();
};

/**
 * Specifies overrides for the default behaviour in Magma.
 */
struct MockMagmaFileSystem {
    /**
     * Constructs a MockMagmaFileSystem which use the specified mock file
     * implementation. The mock file will be constructed with unique_ptr to the
     * real Magma file instance.
     * @param args Will be passed into the mock constructor after the real
     * magma::File pointer
     */
    template <typename FileMock, typename... Args>
    static MockMagmaFileSystem withMockFile(const Args&... args) {
        MockMagmaFileSystem mockFs;
        mockFs.wrapFile = [args...](auto file) -> std::unique_ptr<magma::File> {
            return std::make_unique<FileMock>(std::move(file), args...);
        };
        return mockFs;
    }

    /// This function will be used to open a file.
    std::function<std::unique_ptr<magma::File>(const std::string& path)>
            makeFile{nullptr};
    /// This function will be used to wrap a file object within Magma.
    std::function<std::unique_ptr<magma::File>(
            std::unique_ptr<magma::File> file)>
            wrapFile{nullptr};
    /// This function will be used to open a directory.
    std::function<std::unique_ptr<magma::Directory>(const std::string& path)>
            makeDirectory{nullptr};
    /// This function will be used to wrap a directory object within Magma.
    std::function<std::unique_ptr<magma::Directory>(
            std::unique_ptr<magma::Directory> directory)>
            wrapDirectory{nullptr};
};
