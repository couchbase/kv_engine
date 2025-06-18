/*
 *     Copyright 2024-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */

#include "magma-kvstore_fs.h"

#include "file_ops_tracker.h"

#include <libmagma/file.h>
#include <libmagma/status.h>

class MagmaFile : public magma::File {
public:
    MagmaFile(FileOpsTracker& tracker, std::unique_ptr<magma::File> baseFile)
        : tracker(tracker), baseFile(std::move(baseFile)) {
    }

    magma::Status Open(const int openFlags = OpenFlags::ReadWrite |
                                             OpenFlags::Create) override {
        auto g = tracker.startWithScopeGuard(FileOp::open());
        return baseFile->Open(openFlags);
    }

    magma::Status Size(size_t* size) override {
        auto g = tracker.startWithScopeGuard(FileOp::read(0));
        return baseFile->Size(size);
    }

    magma::Status LastModificationTime(time_t* time) override {
        auto g = tracker.startWithScopeGuard(FileOp::read(0));
        return baseFile->LastModificationTime(time);
    }

    magma::Status Read(int64_t offset,
                       size_t len,
                       char* dst,
                       size_t* n) override {
        auto g = tracker.startWithScopeGuard(FileOp::read(len));
        return baseFile->Read(offset, len, dst, n);
    }

    magma::Status Write(int64_t offset, const char* src, size_t len) override {
        auto g = tracker.startWithScopeGuard(FileOp::write(len));
        return baseFile->Write(offset, src, len);
    }

    magma::Status Sync() override {
        auto g = tracker.startWithScopeGuard(FileOp::sync());
        return baseFile->Sync();
    }

    magma::Status Close() override {
        auto g = tracker.startWithScopeGuard(FileOp::close());
        return baseFile->Close();
    }

    magma::Status Destroy(magma::FileRemover removeAll) override {
        auto g = tracker.startWithScopeGuard(FileOp::write(0));
        return baseFile->Destroy(std::move(removeAll));
    }

    magma::Status Copy(const std::string& newPath) override {
        auto g = tracker.startWithScopeGuard(FileOp::write(0));
        return baseFile->Copy(newPath);
    }

    magma::Status Mmap(char** retPtr) override {
        auto g = tracker.startWithScopeGuard(FileOp::open());
        return baseFile->Mmap(retPtr);
    }

    magma::Status Munmap() override {
        auto g = tracker.startWithScopeGuard(FileOp::close());
        return baseFile->Munmap();
    }

    void SetAccessPattern(AccessPattern ap) override {
        auto g = tracker.startWithScopeGuard(FileOp::write(0));
        baseFile->SetAccessPattern(ap);
    }

    void ReadAhead(int64_t offset, size_t len) override {
        auto g = tracker.startWithScopeGuard(FileOp::read(len));
        baseFile->ReadAhead(offset, len);
    }

    magma::Status CachedSize(size_t offset,
                             size_t len,
                             size_t& cachedBytes) override {
        return baseFile->CachedSize(offset, len, cachedBytes);
    }

    size_t GetBlockSize() override {
        auto g = tracker.startWithScopeGuard(FileOp::read(0));
        return baseFile->GetBlockSize();
    }

    magma::Status Preallocate(size_t size, bool keepSize) override {
        auto g = tracker.startWithScopeGuard(FileOp::write(0));
        return baseFile->Preallocate(size, keepSize);
    }

    magma::Status Truncate(size_t size) override {
        auto g = tracker.startWithScopeGuard(FileOp::write(0));
        return baseFile->Truncate(size);
    }

    std::string GetPath() const override {
        return baseFile->GetPath();
    }

    magma::Status Lock() override {
        auto g = tracker.startWithScopeGuard(FileOp::write(0));
        return baseFile->Lock();
    }

    std::pair<magma::Status, bool> TryLock() override {
        auto g = tracker.startWithScopeGuard(FileOp::write(0));
        return baseFile->TryLock();
    }

    magma::Status Unlock() override {
        auto g = tracker.startWithScopeGuard(FileOp::write(0));
        return baseFile->Unlock();
    }

private:
    FileOpsTracker& tracker;
    std::unique_ptr<magma::File> baseFile;
};

std::unique_ptr<magma::File> getMagmaTrackingFile(
        FileOpsTracker& tracker, std::unique_ptr<magma::File> baseFile) {
    return std::make_unique<MagmaFile>(tracker, std::move(baseFile));
}

class MagmaDirectory : public magma::Directory {
public:
    MagmaDirectory(FileOpsTracker& tracker,
                   std::unique_ptr<magma::Directory> baseDir)
        : tracker(tracker), baseDir(std::move(baseDir)) {
    }

    magma::Status Open(OpenMode mode) override {
        auto g = tracker.startWithScopeGuard(FileOp::open());
        return baseDir->Open(mode);
    }
    magma::Status Sync() override {
        auto g = tracker.startWithScopeGuard(FileOp::sync());
        return baseDir->Sync();
    }

private:
    FileOpsTracker& tracker;
    std::unique_ptr<magma::Directory> baseDir;
};

std::unique_ptr<magma::Directory> getMagmaTrackingDirectory(
        FileOpsTracker& tracker, std::unique_ptr<magma::Directory> baseDir) {
    return std::make_unique<MagmaDirectory>(tracker, std::move(baseDir));
}

magma::FileSystem getMagmaTrackingFileSystem(FileOpsTracker& tracker,
                                             magma::FileSystem baseFs) {
    magma::FileSystem fs;

    Expects(baseFs.MakeFile);
    fs.MakeFile = [&tracker, wrapped = baseFs.MakeFile](
                          const auto& path) -> std::unique_ptr<magma::File> {
        return getMagmaTrackingFile(tracker, wrapped(path));
    };

    Expects(baseFs.MakeDirectory);
    fs.MakeDirectory =
            [&tracker, wrapped = baseFs.MakeDirectory](
                    const auto& path) -> std::unique_ptr<magma::Directory> {
        return getMagmaTrackingDirectory(tracker, wrapped(path));
    };

    // The following members may not be initialised for a read-only FileSystem.

    if (baseFs.Link) {
        fs.Link = [&tracker, wrapped = baseFs.Link](
                          const auto& src, const auto& dst) -> magma::Status {
            auto g = tracker.startWithScopeGuard(FileOp::write(0));
            return wrapped(src, dst);
        };
    }

    if (baseFs.RemoveAllWithCallback) {
        fs.RemoveAllWithCallback =
                [&tracker, wrapped = baseFs.RemoveAllWithCallback](
                        const std::string& path,
                        bool blocking,
                        std::function<void(magma::Status)> callback)
                -> magma::Status {
            auto g = tracker.startWithScopeGuard(FileOp::write(0));
            return wrapped(path, blocking, callback);
        };
    }

    if (baseFs.Rename) {
        fs.Rename = [&tracker, wrapped = baseFs.Rename](
                            const std::string& oldFilePath,
                            const std::string& newFilePath) -> magma::Status {
            auto g = tracker.startWithScopeGuard(FileOp::write(0));
            return wrapped(oldFilePath, newFilePath);
        };
    }

    return fs;
}
