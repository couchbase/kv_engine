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
#pragma once

#include <cstdio>
#include <stdexcept>

#include "memorymap.h"

class Channel {
public:
    class MetaInfo {
    public:
        MetaInfo()
            : offset(0),
              size(0),
              sec(0),
              usec(0) { }

        uint64_t offset;
        uint64_t size;
        uint32_t sec;
        uint32_t usec;

        int contains(uint64_t val) {
            if (val < offset) {
                return -1;
            }

            if (offset <= val && (offset + size) > val) {
                return 0;
            }

            return 1;
        }
    };

    Channel(const std::string& dir,
            const std::string& key,
            const std::string& name)
        : data(NULL),
          meta(NULL),
          metainfo() {
        std::string dirnm = dir + key;
        if (access(dirnm.c_str(), F_OK) == -1) {
            if (mkdir(dirnm.c_str(), S_IRUSR | S_IWUSR | S_IXUSR) == -1) {
                std::stringstream ss;
                ss << "Failed to make directory: " << dirnm << " - " <<
                strerror(errno);
                throw std::runtime_error(ss.str());
            }
        }

        std::string datafile = dirnm + "/" + name + ".pcap";
        data = fopen(datafile.c_str(), "w");
        if (data == NULL) {
            std::stringstream ss;
            ss << "Failed to open " << datafile << " - " << strerror(errno);
            throw std::runtime_error(ss.str());
        }
        std::string metafile = dirnm + "/" + name + ".meta";
        meta = fopen(metafile.c_str(), "w");
        if (meta == NULL) {
            std::stringstream ss;
            ss << "Failed to open " << metafile << " - " << strerror(errno);
            throw std::runtime_error(ss.str());
        }
    }

    void add(Pcap::Packet& p) {
        uint32_t o = (uint32_t) (p.data - p.payload);
        if (p.incl_len != p.orig_len) {
            throw std::runtime_error("Packet truncated");
        }

        if (o < p.incl_len) {
            uint32_t nbytes = p.incl_len - o;
            // @todo validate writes!!!
            fwrite(p.payload + o, 1, nbytes, data);
            metainfo.sec = p.ts_sec;
            metainfo.usec = p.ts_usec;
            metainfo.size = nbytes;
            fwrite(&metainfo, 1, sizeof(metainfo), meta);
            metainfo.offset += nbytes;
        }
    }

    void closeFiles() {
        if (data != NULL) {
            fclose(data);
            data = NULL;
        }
        if (meta != NULL) {
            fclose(meta);
            meta = NULL;
        }
    }

    ~Channel() {
        closeFiles();
    }

private:
    FILE* data;
    FILE* meta;
    MetaInfo metainfo;
};

class MappedChannel {
public:
    void map(const std::string& name) {
        data.create((name + ".pcap").c_str());
        metamapping.create((name + ".meta").c_str());
        nmeta = metamapping.size / sizeof(Channel::MetaInfo);
        if ((metamapping.size % sizeof(Channel::MetaInfo)) != 0) {
            throw std::runtime_error("Incorrect size of .meta file");
        }
        meta = static_cast<Channel::MetaInfo*>(metamapping.root);
    }

    Channel::MetaInfo getMeta(void* ptr) {
        uint64_t offset = (uint64_t) ((uint8_t*) ptr - (uint8_t*) data.root);

        int left = 0;
        int right = nmeta - 1;

        while (left <= right) {
            int center = (left + right) / 2;
            Channel::MetaInfo ret = meta[center];
            int cmp = ret.contains(offset);
            if (cmp > 0) {
                left = center + 1;
            } else if (cmp < 0) {
                right = center - 1;
            } else {
                return ret;
            }
        }

        throw std::runtime_error("index not present!");
    }

protected:
    MemoryMappedFile data;
    MemoryMappedFile metamapping;
    Channel::MetaInfo* meta;
    size_t nmeta;
};

class MemcachedPacket {
public:
    uint8_t* root;
    uint32_t bodylen;

    uint32_t getOpaque(void) const {
        if (*root == 0x80) {
            const protocol_binary_request_header* req = reinterpret_cast<const protocol_binary_request_header*>(root);
            return req->request.opaque;
        } else {
            const protocol_binary_response_header* res = reinterpret_cast<const protocol_binary_response_header*>(root);
            return res->response.opaque;
        }
    }

    Channel::MetaInfo first;
    Channel::MetaInfo last;
};

class MemcachedPacketChannel : public MappedChannel {
public:
    MemcachedPacketChannel()
        : MappedChannel(),
          ptr(NULL),
          remaining(0) { }

    void load(const std::string& name) {
        map(name);
        ptr = static_cast<uint8_t*>(data.root);
        remaining = data.size;
    }

    bool seek(void) {
        while (true) {
            seekForMagic();
            if (isValidHeader()) {
                parse();
                return true;
            }
            ++ptr;
        }

        return false;
    }

    std::list<MemcachedPacket> getPackets(void) {
        return packets;
    }

protected:
    uint8_t* ptr;
    size_t remaining;
    std::list<MemcachedPacket> packets;

    void parse(void) {
        // @todo build up a list of all of the packets
        while (remaining > 23) {
            MemcachedPacket p;
            p.root = ptr;
            p.first = getMeta(ptr);
            uint32_t bodylen;
            memcpy(&bodylen, ptr + 8, 4);
            bodylen = ntohl(bodylen);

            if (remaining < (bodylen + 23)) {
                return;
            }
            p.last = getMeta(ptr + bodylen + 23);
            p.bodylen = bodylen;
            packets.push_back(p);

            remaining -= bodylen + 24;
            ptr += bodylen + 24;
        }
    }

    void seekForMagic(void) {
        while (remaining > 0 && *ptr != 0x80 && *ptr != 0x81) {
            ++ptr;
            --remaining;
        }

        if (remaining < 24) {
            throw std::runtime_error("EOF");
        }
    }

    bool isValidHeader(void) {
        // The extremely basic test is to check that the
        // bodylen > keylen + extlen
        uint32_t bodylen;
        uint16_t keylen;
        uint8_t extlen = *(ptr + 4);
        memcpy(&keylen, ptr + 2, 2);
        memcpy(&bodylen, ptr + 8, 4);
        keylen = ntohs(keylen);
        bodylen = ntohl(bodylen);

        if (static_cast<uint32_t>((keylen + extlen)) > bodylen) {
            return false;
        }

        // And if this is a proper memcached command, we should have
        // a new magic byte at bodylen + 24
        if (remaining > (bodylen + 24)) {
            if (*(ptr + 24 + bodylen) != 0x80 &&
                *(ptr + 24 + bodylen) != 0x81) {
                return false;
            }
        }

        // I should probably add more tests for legal fields in here...
        return true;
    }
};
