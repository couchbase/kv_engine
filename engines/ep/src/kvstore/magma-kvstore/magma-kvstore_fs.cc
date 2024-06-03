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

#include <libmagma/file.h>
#include <libmagma/status.h>

class MagmaFile : public magma::File {
public:
    MagmaFile(std::unique_ptr<magma::File> baseFile)
        : baseFile(std::move(baseFile)) {
    }

    magma::Status Open(const int openFlags = OpenFlags::ReadWrite |
                                             OpenFlags::Create) override {
        return baseFile->Open(openFlags);
    }

    magma::Status Size(size_t* size) override {
        return baseFile->Size(size);
    }

    magma::Status LastModificationTime(time_t* time) override {
        return baseFile->LastModificationTime(time);
    }

    magma::Status Read(int64_t offset,
                       size_t len,
                       char* dst,
                       size_t* n) override {
        return baseFile->Read(offset, len, dst, n);
    }

    magma::Status Write(int64_t offset, const char* src, size_t len) override {
        return baseFile->Write(offset, src, len);
    }

    magma::Status Sync() override {
        return baseFile->Sync();
    }

    magma::Status Close() override {
        return baseFile->Close();
    }

    magma::Status Destroy(magma::FileRemover removeAll) override {
        return baseFile->Destroy(std::move(removeAll));
    }

    magma::Status Copy(const std::string& newPath) override {
        return baseFile->Copy(newPath);
    }

    magma::Status Mmap(char** retPtr) override {
        return baseFile->Mmap(retPtr);
    }

    magma::Status Munmap() override {
        return baseFile->Munmap();
    }

    void SetAccessPattern(AccessPattern ap) override {
        baseFile->SetAccessPattern(ap);
    }

    void ReadAhead(int64_t offset, size_t len) override {
        baseFile->ReadAhead(offset, len);
    }

    magma::Status CachedSize(size_t offset,
                             size_t len,
                             size_t& cachedBytes) override {
        return baseFile->CachedSize(offset, len, cachedBytes);
    }

    size_t GetBlockSize() override {
        return baseFile->GetBlockSize();
    }

    magma::Status Preallocate(size_t size, bool keepSize) override {
        return baseFile->Preallocate(size, keepSize);
    }

    magma::Status Truncate(size_t size) override {
        return baseFile->Truncate(size);
    }

    std::string GetPath() const override {
        return baseFile->GetPath();
    }

    magma::Status Lock() override {
        return baseFile->Lock();
    }

    std::pair<magma::Status, bool> TryLock() override {
        return baseFile->TryLock();
    }

    magma::Status Unlock() override {
        return baseFile->Unlock();
    }

private:
    std::unique_ptr<magma::File> baseFile;
};

std::unique_ptr<magma::File> getMagmaTrackingFile(
        std::unique_ptr<magma::File> baseFile) {
    return std::make_unique<MagmaFile>(std::move(baseFile));
}

class MagmaDirectory : public magma::Directory {
public:
    MagmaDirectory(std::unique_ptr<magma::Directory> baseDir)
        : baseDir(std::move(baseDir)) {
    }

    magma::Status Open(OpenMode mode) override {
        return baseDir->Open(mode);
    }
    magma::Status Sync() override {
        return baseDir->Sync();
    }

    magma::Status RemoteOpen(const std::string& remote) override {
        return baseDir->RemoteOpen(remote);
    }

    magma::Status QueueRemoteSync(std::vector<PathAndSize>& fileList) override {
        return baseDir->QueueRemoteSync(fileList);
    }

    std::tuple<bool, magma::Status> GetRemoteSyncStatus() override {
        return baseDir->GetRemoteSyncStatus();
    }

    magma::Status RequestRemoteSyncCompletion() override {
        return baseDir->RequestRemoteSyncCompletion();
    }

private:
    std::unique_ptr<magma::Directory> baseDir;
};

std::unique_ptr<magma::Directory> getMagmaTrackingDirectory(
        std::unique_ptr<magma::Directory> baseDir) {
    return std::make_unique<MagmaDirectory>(std::move(baseDir));
}

magma::FileSystem getMagmaTrackingFileSystem(magma::FileSystem baseFs) {
    magma::FileSystem fs;

    Expects(baseFs.MakeFile);
    fs.MakeFile = [wrapped = baseFs.MakeFile](
                          const auto& path) -> std::unique_ptr<magma::File> {
        return getMagmaTrackingFile(wrapped(path));
    };

    Expects(baseFs.MakeDirectory);
    fs.MakeDirectory =
            [wrapped = baseFs.MakeDirectory](
                    const auto& path) -> std::unique_ptr<magma::Directory> {
        return getMagmaTrackingDirectory(wrapped(path));
    };

    // The following members may not be initialised for a read-only FileSystem.

    if (fs.Link) {
        fs.Link = [wrapped = baseFs.Link](const auto& src,
                                          const auto& dst) -> magma::Status {
            return wrapped(src, dst);
        };
    }

    if (baseFs.RemoveAllWithCallback) {
        fs.RemoveAllWithCallback =
                [wrapped = baseFs.RemoveAllWithCallback](
                        const std::string& path,
                        bool blocking,
                        std::function<void(magma::Status)> callback)
                -> magma::Status { return wrapped(path, blocking, callback); };
    }

    if (baseFs.Rename) {
        fs.Rename = [wrapped = baseFs.Rename](
                            const std::string& oldFilePath,
                            const std::string& newFilePath) -> magma::Status {
            return wrapped(oldFilePath, newFilePath);
        };
    }

    return fs;
}
