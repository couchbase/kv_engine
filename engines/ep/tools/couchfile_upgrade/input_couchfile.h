/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2018-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */

#pragma once

#include "couchfile.h"

#include <optional>

namespace Collections {

class OutputCouchFile;
class InputCouchFile : public CouchFile {
public:
    InputCouchFile(OptionsSet options, const std::string& filename);

    /**
     * Perform some checks on the input file.
     * Check it's not already marked as namespaced (or partially processed)
     */
    enum class PreflightStatus {
        ReadyForUpgrade,
        UpgradePartial,
        UpgradeComplete,
        UpgradeCompleteAndPartial,
        InputFileCannotBeProcessed
    };

    /// @return a PreflightStatus for the file
    PreflightStatus preflightChecks(std::ostream& os) const;

    /**
     * Upgrade this input couchfile and send the new file to output couchfile
     */
    void upgrade(OutputCouchFile& output) const;

    std::string getLocalDocument(const std::string& documentName) const;

private:
    bool doesLocalDocumentExist(const std::string& documentName) const;

    bool isCompletelyNamespaced() const;

    bool isPartiallyNamespaced() const;

    std::optional<bool> getSupportsNamespaces() const;

    LocalDocPtr openLocalDocument(const std::string& documentName) const;
};
} // end namespace Collections
