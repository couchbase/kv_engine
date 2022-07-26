/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2022-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */

/*
 * humpty_dumpty
 *
 * A program which lets you experiment with different requests against
 * different failover tables and how ep-engine will respond.
 */

#include "configuration.h"
#include "failover-table.h"
#include <fstream>
#include <iostream>
#include <sstream>
#include "boost/algorithm/string/classification.hpp"
#include "boost/algorithm/string/split.hpp"

int main(int argc, char** argv) {
    if (argc != 4) {
        std::cerr << "Usage: " << argv[0]
                  << " <failover_table.json> <high_seqno> <purge_seqno>\n";

        std::cerr << R"(
A program which lets you experiment with different
stream requests against different failover tables and how ep-engine will
respond.

It requires 3 arguments to specify the (simulated) state of ep-engine:
    1. A failover table encoded as a JSON array of objects, where each object
       has an "id" and "seq" element representing an the UUID, seqno pair of
       each entry in the failover table. Entries must be ordered in descending
       seqno (time) order - i.e. most recent entry must appear first.
       Additional fields are ignored, so comments can be added with a "comment":
       element.
       Example file contents:

       [
           {"id": 3333, "seq": 80, "comment": "latest entry" },
           {"id": 2222, "seq": 50 },
           {"id": 1111, "seq": 0 }
       ]

    2. The vBucket high seqno. This must be greater than or equal to all
       entries in the failover table; any entries which are less than the
       high_seqno are considered erroneous and are removed when table is
       parsed and created.
    3. The vBucket purge seqno.

To test the behaviour of a particular stream request, specify the four
properties of the request as space-separated numbers on stdin. Multiple stream
requests can be passed by placing each on a single line. For example, to issue
a request for UUID:1111, with snapshot range {0,10} and start seqno 8:

    1111 0 10 8
)";
        return EXIT_FAILURE;
    }
    Configuration config;

    std::ifstream jsonFile{argv[1]};
    const auto json = nlohmann::json::parse(jsonFile);
    const int64_t highSeqno = std::strtoul(argv[2], nullptr, 10);
    const int64_t purgeSeqno = std::strtoul(argv[3], nullptr, 10);

    FailoverTable table{json, config.getMaxFailoverEntries(), highSeqno};

    std::cout << "Simulating behaviour of VBucket with highSeqno: " << highSeqno
              << ", purgeSeqno:" << purgeSeqno << ", failoverTable:\n[\n";
    for (const auto& entry : table.getJSON()) {
        std::cout << "    " << entry << "\n";
    }
    std::cout << "]\n\n";

    for (std::string line; std::getline(std::cin, line);) {
        std::vector<std::string> fields;
        boost::algorithm::split(fields,
                                line,
                                boost::is_any_of("\t "),
                                boost::token_compress_on);
        if (fields.size() != 4) {
            std::cerr
                    << "Warning: expected 4 fields for testing failover table: "
                       "<UUID> <snapStart> <snapEnd> <start> - skipping input.";
            continue;
        }

        uint64_t uuid = std::stoull(fields.at(0));
        uint64_t snapStart = std::stoull(fields.at(1));
        uint64_t snapEnd = std::stoull(fields.at(2));
        uint64_t start = std::stoull(fields.at(3));

        std::cout << "Testing UUID:" << uuid << " snapshot:{" << snapStart
                  << "," << snapEnd << "} start:" << start << "\n";

        auto result = table.needsRollback(start,
                                          highSeqno,
                                          uuid,
                                          snapStart,
                                          snapEnd,
                                          purgeSeqno,
                                          false,
                                          {});
        std::cout << "  Rollback:" << (result ? "true" : "false") << "\n";
        if (result) {
            std::cout << "  Requested rollback seqno:" << result->rollbackSeqno
                      << "\n";
            std::cout << "  Reason: " << result->rollbackReason << "\n";
        }
    }
    return EXIT_SUCCESS;
}
