/*
 *     Copyright 2026-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */

#pragma once

#include <fmt/format.h>
#include <memcached/dockey_view.h>
#include <memcached/protocol_binary.h>
#include <nlohmann/json.hpp>
#include <cstddef>
#include <optional>
#include <stdexcept>
#include <string_view>

namespace cb::mcbp {

/**
 * DcpCacheTransferBuffer provides iteration over a buffer containing
 * serialized DCP cache transfer items.
 *
 * The buffer format is a sequence of one or more:
 *   [DcpCacheTransferPayload][key bytes][value bytes]
 *
 * Where:
 *   - DcpCacheTransferPayload is a structure containing metadata
 *   - key bytes is getKeyLen() bytes of key data
 *   - value bytes is getValueLen() bytes of value data (may be 0)
 *
 * The class provides an iterator interface for traversing the items.
 * Validation is performed during iteration rather than construction,
 * except for a minimal size check in the constructor.
 */
class DcpCacheTransferBuffer {
public:
    /**
     * Represents a single item in the buffer.
     * Provides access to the payload metadata and key/value views.
     */
    class Item {
    public:
        Item(const request::DcpCacheTransferPayload& payload,
             std::string_view key,
             std::string_view value)
            : payload(payload), key(key), value(value) {
        }

        const request::DcpCacheTransferPayload& getPayload() const {
            return payload;
        }

        std::string_view getKey() const {
            return key;
        }

        std::string_view getValue() const {
            return value;
        }

        uint64_t getCas() const {
            return payload.getCas();
        }

        uint64_t getBySeqno() const {
            return payload.getBySeqno();
        }

        uint64_t getRevSeqno() const {
            return payload.getRevSeqno();
        }

        uint32_t getFlags() const {
            return payload.getFlags();
        }

        uint32_t getExpiration() const {
            return payload.getExpiration();
        }

        uint8_t getDatatype() const {
            return payload.getDatatype();
        }

        uint8_t getCacheHint() const {
            return payload.getCacheHint();
        }

    private:
        const request::DcpCacheTransferPayload& payload;
        std::string_view key;
        std::string_view value;
    };

    /**
     * Iterator for traversing items in the buffer.
     * Validation is performed when advancing the iterator.
     */
    class Iterator {
    public:
        using iterator_category = std::input_iterator_tag;
        using value_type = Item;
        using difference_type = std::ptrdiff_t;
        using pointer = const Item*;
        using reference = const Item&;

        Iterator() = default;

        explicit Iterator(std::string_view data) : remaining(data) {
            if (!remaining.empty()) {
                advance();
            }
        }

        const Item& operator*() const {
            if (!current) {
                throw std::logic_error(
                        "DcpCacheTransferBuffer::Iterator: "
                        "dereferencing invalid iterator");
            }
            return *current;
        }

        const Item* operator->() const {
            return &(*current);
        }

        Iterator& operator++() {
            advance();
            return *this;
        }

        Iterator operator++(int) {
            Iterator pre = *this;
            advance();
            return pre;
        }

        bool operator==(const Iterator& other) const {
            // Two iterators are equal if they both have no current item
            // (i.e., both are at end or invalid)
            if (!current && !other.current) {
                return true;
            }
            // Otherwise, they're equal if they point to the same position
            return remaining.data() == other.remaining.data() &&
                   remaining.size() == other.remaining.size();
        }

        bool operator!=(const Iterator& other) const {
            return !(*this == other);
        }

        /**
         * Check if iteration encountered an error.
         * @return true if an error occurred during iteration
         */
        bool hasError() const {
            return !errorContext.is_null();
        }

        /**
         * Get structured error context if iteration failed. Fields describe
         * what went wrong (see advance() for the schema of each variant).
         * @return The error context, or a null json object if no error
         */
        const nlohmann::json& getError() const {
            return errorContext;
        }

    private:
        void advance() {
            current.reset();

            if (remaining.empty()) {
                return;
            }

            constexpr size_t payloadSize =
                    sizeof(request::DcpCacheTransferPayload);

            // Check if we have enough data for the payload header
            if (remaining.size() < payloadSize) {
                setError({{"reason", "insufficient_payload_data"},
                          {"need", payloadSize},
                          {"have", remaining.size()}});
                remaining = {};
                return;
            }

            // Get pointer to the payload (it's already in the buffer)
            const auto* payload =
                    reinterpret_cast<const request::DcpCacheTransferPayload*>(
                            remaining.data());

            const auto keyLen = payload->getKeyLen();
            const auto valueLen = payload->getValueLen();

            // Key must be at least 2 bytes (collection prefix + 1 byte minimum)
            // and no larger than MaxCollectionsKeyLen.
            // See McbpValidator::is_document_key_valid
            constexpr uint16_t minKeyLen = 2;
            if (keyLen < minKeyLen) {
                setError({{"reason", "invalid_key_length"},
                          {"key_len", keyLen},
                          {"min_key_len", minKeyLen}});
                remaining = {};
                return;
            }
            if (keyLen > MaxCollectionsKeyLen) {
                setError({{"reason", "invalid_key_length"},
                          {"key_len", keyLen},
                          {"max_key_len", MaxCollectionsKeyLen}});
                remaining = {};
                return;
            }

            const size_t totalItemSize = payloadSize + keyLen + valueLen;

            // Validate we have enough data for this complete item
            if (remaining.size() < totalItemSize) {
                setError({{"reason", "insufficient_item_data"},
                          {"key_len", keyLen},
                          {"value_len", valueLen},
                          {"need", totalItemSize},
                          {"have", remaining.size()}});
                remaining = {};
                return;
            }

            // Extract key and value views
            const char* keyStart = remaining.data() + payloadSize;
            std::string_view key(keyStart, keyLen);
            std::string_view value(keyStart + keyLen, valueLen);

            // Create the current item
            current.emplace(*payload, key, value);

            // Advance past this item
            remaining = remaining.substr(totalItemSize);
        }

        void setError(nlohmann::json ctx) {
            errorContext = std::move(ctx);
        }

        std::string_view remaining;
        std::optional<Item> current;
        nlohmann::json errorContext;
    };

    /**
     * Construct a DcpCacheTransferBuffer from a data view.
     *
     * @param data The buffer containing serialized cache transfer items
     * @throws std::invalid_argument if data is too small to contain at least
     *         one DcpCacheTransferPayload header
     */
    explicit DcpCacheTransferBuffer(std::string_view data) : data(data) {
        if (data.size() < minSize()) {
            throw std::invalid_argument(
                    fmt::format("DcpCacheTransferBuffer: data too small, "
                                "have {} bytes, need at least {} bytes",
                                data.size(),
                                minSize()));
        }
    }

    /**
     * Get the minimum valid buffer size.
     * This is one payload header plus a minimal key (collection prefix + 1
     * byte) and no value. See McbpValidator::is_document_key_valid for min key
     * validation.
     */
    constexpr static size_t minSize() {
        return sizeof(request::DcpCacheTransferPayload) + 2;
    }

    /**
     * Get an iterator to the first item.
     */
    Iterator begin() const {
        return Iterator(data);
    }

    /**
     * Get an iterator past the last item.
     */
    Iterator end() const {
        return {};
    }

    /**
     * Get the raw data view.
     */
    std::string_view getData() const {
        return data;
    }

    /**
     * Get the size of the buffer in bytes.
     */
    size_t size() const {
        return data.size();
    }

    /**
     * Check if the buffer is empty.
     * Note: A valid buffer always has at least one payload header.
     */
    bool empty() const {
        return data.empty();
    }

private:
    std::string_view data;
};

} // namespace cb::mcbp
