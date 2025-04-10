/*
 *     Copyright 2025-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */

#include "mock_magma_filesystem.h"
#include <folly/portability/GMock.h>
#include <libmagma/status.h>

MockMagmaFile::MockMagmaFile(std::unique_ptr<File> wrapped)
    : wrapped(std::move(wrapped)) {
    delegateToWrapped();
}

bool MockMagmaFile::isWal() const {
    return GetPath().find("/wal/") != std::string::npos;
}

bool MockMagmaFile::isSSTable() const {
    return GetPath().find("/sstable.") != std::string::npos &&
           GetPath().ends_with(".data");
}

bool MockMagmaFile::isKeyIndex() const {
    return GetPath().find("/keyIndex/") != std::string::npos;
}

bool MockMagmaFile::isLocalIndex() const {
    return GetPath().find("/localIndex/") != std::string::npos;
}

bool MockMagmaFile::isSeqIndex() const {
    return GetPath().find("/seqIndex/") != std::string::npos;
}

void MockMagmaFile::delegateToWrapped() {
    using namespace ::testing;

    ON_CALL(*this, Open(_))
            .WillByDefault(Invoke(wrapped.get(), &magma::File::Open));
    ON_CALL(*this, Size(_))
            .WillByDefault(Invoke(wrapped.get(), &magma::File::Size));
    ON_CALL(*this, LastModificationTime(_))
            .WillByDefault(
                    Invoke(wrapped.get(), &magma::File::LastModificationTime));
    ON_CALL(*this, Read(_, _, _, _))
            .WillByDefault(Invoke(wrapped.get(), &magma::File::Read));
    ON_CALL(*this, Write(_, _, _))
            .WillByDefault(Invoke(wrapped.get(), &magma::File::Write));
    ON_CALL(*this, Sync())
            .WillByDefault(Invoke(wrapped.get(), &magma::File::Sync));
    ON_CALL(*this, Close())
            .WillByDefault(Invoke(wrapped.get(), &magma::File::Close));
    ON_CALL(*this, Destroy(_))
            .WillByDefault(Invoke(wrapped.get(), &magma::File::Destroy));
    ON_CALL(*this, Copy(_))
            .WillByDefault(Invoke(wrapped.get(), &magma::File::Copy));
    ON_CALL(*this, Mmap(_))
            .WillByDefault(Invoke(wrapped.get(), &magma::File::Mmap));
    ON_CALL(*this, Munmap())
            .WillByDefault(Invoke(wrapped.get(), &magma::File::Munmap));
    ON_CALL(*this, SetAccessPattern(_))
            .WillByDefault(
                    Invoke(wrapped.get(), &magma::File::SetAccessPattern));
    ON_CALL(*this, ReadAhead(_, _))
            .WillByDefault(Invoke(wrapped.get(), &magma::File::ReadAhead));
    ON_CALL(*this, CachedSize(_, _, _))
            .WillByDefault(Invoke(wrapped.get(), &magma::File::CachedSize));
    ON_CALL(*this, GetBlockSize())
            .WillByDefault(Invoke(wrapped.get(), &magma::File::GetBlockSize));
    ON_CALL(*this, Preallocate(_, _))
            .WillByDefault(Invoke(wrapped.get(), &magma::File::Preallocate));
    ON_CALL(*this, Truncate(_))
            .WillByDefault(Invoke(wrapped.get(), &magma::File::Truncate));
    ON_CALL(*this, GetPath())
            .WillByDefault(Invoke(wrapped.get(), &magma::File::GetPath));
    ON_CALL(*this, Lock())
            .WillByDefault(Invoke(wrapped.get(), &magma::File::Lock));
    ON_CALL(*this, TryLock())
            .WillByDefault(Invoke(wrapped.get(), &magma::File::TryLock));
    ON_CALL(*this, Unlock())
            .WillByDefault(Invoke(wrapped.get(), &magma::File::Unlock));
}

MockMagmaDirectory::MockMagmaDirectory(std::unique_ptr<Directory> wrapped)
    : wrapped(std::move(wrapped)) {
    delegateToWrapped();
}

void MockMagmaDirectory::delegateToWrapped() {
    using namespace ::testing;

    ON_CALL(*this, Open(_))
            .WillByDefault(Invoke(wrapped.get(), &magma::Directory::Open));
    ON_CALL(*this, Sync())
            .WillByDefault(Invoke(wrapped.get(), &magma::Directory::Sync));
}
