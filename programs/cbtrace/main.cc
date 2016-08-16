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

/**
 * cbtrace is a program used to read pcap files and pick
 * out the memcached protocol frames and perform operations on them.
 *
 * It was originally hacked together in order to look at some slow
 * response times, but may be useful for people in the future so its
 * source is included here (but the tool is not installed due to its
 * really rough edges (and may very likely coredump)
 */

#include <algorithm>
#include <unistd.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <getopt.h>
#include <cstdlib>
#include <cstdio>
#include <cstring>
#include <iostream>
#include <string>

#ifdef WIN32
#include <windows.h>
#include <direct.h>
#include <io.h>
#define mkdir(a, b) _mkdir(a)
#define F_OK 0
#define snprintf _snprintf
#define access _access
#else

#include <arpa/inet.h>

#endif

#include <sstream>
#include <ctype.h>
#include <inttypes.h>

#include <map>
#include <list>

#include <memcached/protocol_binary.h>
#include <platform/cbassert.h>
#include <platform/dirutils.h>
#include <utilities/protocol2text.h>

#include "pcap.h"
#include "channel.h"

static bool packetdump = false;

typedef struct timings_st {
    uint32_t total;
    uint32_t max;

    /* We collect timings per 10usec */
    uint32_t usec[100];

    /* we collect timings from 0-49 ms (entry 0 is never used!) */
    uint32_t msec[50];

    uint32_t halfsec[10];

    uint32_t wayout;
} timings_t;

static void callback(timings_t* t, const char* timeunit, uint32_t min,
                     uint32_t max, uint32_t total) {
    if (total > 0) {
        int ii;
        char buffer[1024];
        int offset;
        int num;
        if (min > 0 && max == 0) {
            offset = sprintf(buffer, "[%4u - inf.]%s", min, timeunit);
        } else {
            offset = sprintf(buffer, "[%4u - %4u]%s", min, max, timeunit);
        }
        num = static_cast<int>((float) 40.0 * (float) total / (float) t->max);
        offset += sprintf(buffer + offset, " |");
        for (ii = 0; ii < num; ++ii) {
            offset += sprintf(buffer + offset, "#");
        }

        sprintf(buffer + offset, " - %u (%02.02f%%)\n", total,
                (((float) total / t->total) * 100));
        fputs(buffer, stdout);
    }
}

static void histogram(timings_t* t) {
    int ii;
    for (ii = 0; ii < 100; ++ii) {
        callback(t, "us", ii * 10, ((ii + 1) * 10 - 1), t->usec[ii]);
    }

    for (ii = 1; ii < 50; ++ii) {
        callback(t, "ms", ii, ii, t->msec[ii]);
    }

    for (ii = 0; ii < 10; ++ii) {
        callback(t, "ms", ii * 500, ((ii + 1) * 500) - 1, t->halfsec[ii]);
    }

    callback(t, "ms", (9 * 500), 0, t->wayout);

}


std::map<std::string, Channel*> channels;

std::string ipToString(uint8_t src[4], uint16_t port) {
    char buf[40];
    snprintf(buf, sizeof(buf), "%u.%u.%u.%u#%u",
             src[0], src[1], src[2], src[3], port);
    return buf;
}

std::string getkey(std::string a, std::string b) {
    std::string ret;

    if (a < b) {
        ret = a + "-" + b;
    } else {
        ret = b + "-" + a;
    }
    return ret;
}

std::ostream& operator<<(std::ostream& out, const Pcap::Header& header) {
    out << "  PCAP file version: " << header.major << "." << header.minor
    << std::endl
    << "  Max packet length: " << header.snaplen << std::endl
    << "  Network          : Ethernet" << std::endl;

    return out;
}

void printByte(std::ostream& out, uint8_t b, bool inBody) {
    int val = ((int) (b >> 4)) & 0x0f;
    val <<= 4;
    val |= (b & 0x0f);

    char buffer[80];
    snprintf(buffer, sizeof(buffer), " 0x%02x", val);
    out << buffer;

    if (inBody && isprint(b)) {
        char buffer[80];
        snprintf(buffer, sizeof(buffer), " ('%c')", (char) b);
        out << buffer;
    } else {
        out << "      ";
    }
    out << "    |";
}

std::ostream& operator<<(std::ostream& out, const MemcachedPacket& p) {
    out << p.first.sec << ":" << p.first.usec;
    if (p.first.sec != p.last.sec || p.first.usec != p.last.usec) {
        out << " - " << p.last.sec << ":" << p.last.usec;
    }

    out << std::endl
        << "      Byte/     0       |       1       |       2       |       3       |"
        << std::endl
        << "         /              |               |               |               |"
        << std::endl
        << "        |0 1 2 3 4 5 6 7|0 1 2 3 4 5 6 7|0 1 2 3 4 5 6 7|0 1 2 3 4 5 6 7|";

    uint32_t ii = 0;
    uint32_t remaining = p.bodylen + 24;

    for (ii = 0; ii < remaining; ++ii) {
        if (ii % 4 == 0) {
            out << std::endl
                << "        +---------------+---------------+---------------+---------------+"
                << std::endl;
            char buffer[30];
            snprintf(buffer, sizeof(buffer), "     %3d|", ii);
            out << buffer;
        }

        printByte(out, p.root[ii], ii > 23);
    }

    out << std::endl;
    if (ii % 4 != 0) {
        out << "        ";
        for (uint32_t jj = 0; jj < ii % 4; ++jj) {
            out << "+---------------";
        }
        out << "+" << std::endl;
    } else {
        out <<
        "        +---------------+---------------+---------------+---------------+" <<
        std::endl;
    }

    if (p.root[0] == 0x80) {
        char buffer[80];
        protocol_binary_request_header* req = reinterpret_cast<protocol_binary_request_header*>(p.root);

        const char* cmd = memcached_opcode_2_text(req->request.opcode);
        if (cmd) {
            out << "    " << cmd << " command" << std::endl;
        } else {
            snprintf(buffer, sizeof(buffer), "%02x", req->request.opcode);
            out << "    " << buffer << " command" << std::endl;
        }
        snprintf(buffer, sizeof(buffer), "    Field        (offset) (value)");
        out << buffer << std::endl;
        snprintf(buffer, sizeof(buffer), "    Magic        (0)    : 0x%02x",
                 req->request.magic);
        out << buffer << std::endl;
        snprintf(buffer, sizeof(buffer), "    Opcode       (1)    : 0x%02x",
                 req->request.opcode);
        out << buffer << std::endl;
        snprintf(buffer, sizeof(buffer),
                 "    Key length   (2,3)  : 0x%04x (%u)",
                 req->request.keylen, ntohs(req->request.keylen));
        out << buffer << std::endl;
        snprintf(buffer, sizeof(buffer), "    Extra length (4)    : 0x%02x",
                 req->request.extlen);
        out << buffer << std::endl;
        snprintf(buffer, sizeof(buffer), "    Data type    (5)    : 0x%02x",
                 req->request.datatype);
        out << buffer << std::endl;
        snprintf(buffer, sizeof(buffer),
                 "    Vbucket      (6,7)  : 0x%04x (%u)",
                 req->request.vbucket,
                 ntohs(req->request.vbucket));
        out << buffer << std::endl;
        snprintf(buffer, sizeof(buffer),
                 "    Total body   (8-11) : 0x%08x (%u)",
                 req->request.bodylen, ntohl(req->request.bodylen));
        out << buffer << std::endl;
        snprintf(buffer, sizeof(buffer), "    Opaque       (12-15): 0x%08x",
                 req->request.opaque);
        out << buffer << std::endl;
        snprintf(buffer, sizeof(buffer),
                 "    CAS          (16-23): 0x%016" PRIx64,
                 req->request.cas);
        out << buffer << std::endl;
    } else {
        char buffer[80];
        protocol_binary_response_header* res = reinterpret_cast<protocol_binary_response_header*>(p.root);
        const char* cmd = memcached_opcode_2_text(res->response.opcode);
        if (cmd) {
            out << "    " << cmd << " response" << std::endl;
        } else {
            snprintf(buffer, sizeof(buffer), "%02x", res->response.opcode);
            out << "    " << buffer << " response" << std::endl;
        }

        snprintf(buffer, sizeof(buffer), "    Field        (offset) (value)");
        out << buffer << std::endl;
        snprintf(buffer, sizeof(buffer), "    Magic        (0)    : 0x%02x",
                 res->response.magic);
        out << buffer << std::endl;
        snprintf(buffer, sizeof(buffer), "    Opcode       (1)    : 0x%02x",
                 res->response.opcode);
        out << buffer << std::endl;
        snprintf(buffer, sizeof(buffer),
                 "    Key length   (2,3)  : 0x%04x (%u)",
                 res->response.keylen, ntohs(res->response.keylen));
        out << buffer << std::endl;
        snprintf(buffer, sizeof(buffer), "    Extra length (4)    : 0x%02x",
                 res->response.extlen);
        out << buffer << std::endl;
        snprintf(buffer, sizeof(buffer), "    Data type    (5)    : 0x%02x",
                 res->response.datatype);
        out << buffer << std::endl;
        snprintf(buffer, sizeof(buffer),
                 "    Status       (6,7)  : 0x%04x (%u)",
                 res->response.status, ntohs(res->response.status));
        out << buffer << std::endl;
        snprintf(buffer, sizeof(buffer),
                 "    Total body   (8-11) : 0x%08x (%u)",
                 res->response.bodylen, ntohl(res->response.bodylen));
        out << buffer << std::endl;
        snprintf(buffer, sizeof(buffer), "    Opaque       (12-15): 0x%08x",
                 res->response.opaque);
        out << buffer << std::endl;
        snprintf(buffer, sizeof(buffer),
                 "    CAS          (16-23): 0x%016" PRIx64,
                 res->response.cas);
        out << buffer << std::endl;
    }

    out << std::endl;
    return out;
}

void packetIpv4(const std::string& cachedir, Pcap::Packet& packet) {
    uint8_t* ptr = packet.payload + 14;
    uint8_t ihl = *ptr & 0x0f;
    uint8_t* payload = ptr + (ihl * 4);

    cb_assert((*ptr & 0xf0) == 0x40);

    packet.ip = ptr;
    packet.tcp = payload;

    switch (ptr[9]) {
    case 0x06: {
        uint16_t src;
        uint16_t dst;

        memcpy(&src, packet.tcp, 2);
        memcpy(&dst, packet.tcp + 2, 2);
        src = ntohs(src);
        dst = ntohs(dst);

        // @todo the port number should be in .ptracerc

        if (src != 11210 && dst != 11210) {
            // We don't care about this packet
            return;
        }

        uint8_t dataOffset = (packet.tcp[12] & 0xf0) >> 4;
        packet.data = packet.tcp + (dataOffset * 4);
        std::string source = ipToString(packet.ip + 12, src);
        std::string destination = ipToString(packet.ip + 16, dst);
        std::string key = source + "-" + destination;

        Channel* c = channels[key];
        if (c == NULL) {
            c = channels[key] = new Channel(cachedir,
                                            getkey(source, destination),
                                            source);
        }
        c->add(packet);
    }
        break;
    case 0x11:
        // udp
        break;

    default:
        fprintf(stderr, " Packet type : 0x%0x\n", packet.payload[23]);
    }
}

static void createCache(std::string& cachedir, const std::string file) {
    Pcap::Parser pcap;

    pcap.create(file.c_str());

    std::cout << "Header: " << std::endl << pcap.getHeader() << std::endl;

    if (pcap.getHeader().snaplen != 65535) {
        throw std::runtime_error(
            "Packet capture with truncated packets is not supported");
    }

    mkdir(cachedir.c_str(), S_IRUSR | S_IWUSR | S_IXUSR);
    cachedir += "/";

    std::cout << "Counting packets..";
    std::cout.flush();

    int ii = 0;
    Pcap::Packet packet;
    while (pcap.next(packet)) {
        ++ii;
        if (packet.incl_len > 14) {
            uint16_t type;
            memcpy(&type, packet.payload + 12, 2);
            type = ntohs(type);
            switch (type) {
            case 0x0800:
                packetIpv4(cachedir, packet);
                break;
            case 0x86dd:
                // IPv6
                break;
            default:;
                // Unknown
                // std::cerr << "Invalid packet type! " << type << std::endl;
                // fprintf(stderr, "0x%04x\n", type);
            }
        }
    }

    std::map<std::string, Channel*>::iterator iter;
    for (iter = channels.begin(); iter != channels.end(); ++iter) {
        delete iter->second;
    }

    std::cout << "\rCapture containing " << ii << " packets" << std::endl
    << "@todo to dump meta information somewhere" << std::endl;
}

class MemcachedStream {
public:
    MemcachedStream(const std::string& dir) {
        channels[0] = NULL;
        channels[1] = NULL;
        memset(&timings, 0, sizeof(timings));

        std::vector<std::string> files;
        files = CouchbaseDirectoryUtilities::findFilesWithPrefix(dir, "");
        int ii = 0;
        for (std::vector<std::string>::iterator iter = files.begin();
             iter != files.end();
             ++iter) {

            size_t idx = iter->rfind(".pcap");

            if (idx != std::string::npos && (idx + 5) == iter->size()) {
                if (ii == 2) {
                    throw std::runtime_error(
                        "WTF more than two channels in a stream");
                }
                iter->resize(idx);

                MemcachedPacketChannel* channel = new MemcachedPacketChannel();
                channel->load(*iter);
                channels[ii++] = channel;
            }
        }

        if (ii != 2) {
            throw std::runtime_error("I don't have both directions");
        }


        try {
            channels[0]->seek();
        } catch (std::runtime_error& e) {
            std::cerr << "WTF: " << e.what();
            return ;
        }

        try {
            channels[1]->seek();
        } catch (std::runtime_error& e) {
            std::cerr << "WTF: " << e.what();
            return ;
        }

        parse();
    }

    void parse(void) {
        std::list<MemcachedPacket> packets[2];
        if (*channels[0]->getPackets().front().root == 0x80) {
            packets[0] = channels[0]->getPackets();
            packets[1] = channels[1]->getPackets();
        } else {
            packets[0] = channels[1]->getPackets();
            packets[1] = channels[0]->getPackets();
        }

        std::list<MemcachedPacket>::iterator a = packets[0].begin();
        std::list<MemcachedPacket>::iterator b = packets[1].begin();

        // spool up to where we have the same opaque..
        if (a == packets[0].end() || b == packets[1].end()) {
            std::cout << "EOF" << std::endl;
        }

        while (a->getOpaque() != b->getOpaque()) {
            ++b;
            std::cout << "skipping" << std::endl;
            if (b == packets[1].end()) {
                std::cout << "EOF" << std::endl;
                return;
            }
        }

        while (a != packets[0].end() && b != packets[1].end()) {
            uint64_t start = a->last.usec;
            uint64_t end = ((b->last.sec - a->last.sec) * (uint64_t) 1000000) +
                           b->last.usec;
            if (end < start) {
                return;
            }
            cb_assert(start <= end);
            collect(a->root[1], end - start);

            if (packetdump) {
                std::cout << *a << *b;
            }

            // @todo fix multi-packet-response
            ++a;
            ++b;
        }
    }

    void dump(void) {
        for (int ii = 0; ii < 0x100; ++ii) {
            if (timings[ii].total) {
                const char* name = memcached_opcode_2_text((uint8_t) ii);
                if (name) {
                    fprintf(stdout, "Dump of %s (%u)\n",
                            name, timings[ii].total);
                } else {
                    fprintf(stdout, "Dump of %02x (%u)\n",
                            (uint8_t) ii, timings[ii].total);
                }
                histogram(timings + ii);
            }
        }
    }

    void dump(const std::list<uint8_t>& cmds) {
        std::list<uint8_t>::const_iterator iter;
        for (iter = cmds.begin(); iter != cmds.end(); ++iter) {
            uint8_t ii = *iter;
            if (timings[ii].total) {
                const char* name = memcached_opcode_2_text(ii);
                if (name) {
                    fprintf(stdout, "Dump of %s (%u)\n",
                            name, timings[ii].total);
                } else {
                    fprintf(stdout, "Dump of %02x (%u)\n",
                            ii, timings[ii].total);
                }
                histogram(timings + ii);
            }
        }
    }


    ~MemcachedStream() {
        delete channels[0];
        delete channels[1];
    }

    void aggregate(timings_t t[0x100]) {
        for (int ii = 0; ii < 0x100; ++ii) {
            t[ii].total += timings[ii].total;
            for (int jj = 0; jj < 100; ++jj) {
                t[ii].usec[jj] += timings[ii].usec[jj];
                if (t[ii].max < t[ii].usec[jj]) {
                    t[ii].max = t[ii].usec[jj];
                }
            }

            for (int jj = 0; jj < 50; ++jj) {
                t[ii].msec[jj] += timings[ii].msec[jj];
                if (t[ii].max < t[ii].msec[jj]) {
                    t[ii].max = t[ii].msec[jj];
                }
            }

            for (int jj = 0; jj < 10; ++jj) {
                t[ii].halfsec[jj] += timings[ii].halfsec[jj];
                if (t[ii].max < t[ii].halfsec[jj]) {
                    t[ii].max = t[ii].halfsec[jj];
                }
            }

            t[ii].wayout += timings[ii].wayout;
            if (t[ii].max < t[ii].wayout) {
                t[ii].max = t[ii].wayout;
            }
        }
    }

private:
    MemcachedPacketChannel* channels[2];

    timings_t timings[0x100];

    void collect(uint8_t cmd, uint64_t usec) {
        timings_t* t = &timings[cmd];
        uint64_t msec = usec / 1000;
        uint64_t hsec = msec / 500;

        t->total++;
        if (usec < 1000) {
            t->usec[usec / 10]++;
            if (t->max < t->usec[usec / 10]) {
                t->max = t->usec[usec / 10];
            }
        } else if (msec < 50) {
            t->msec[msec]++;
            if (t->max < t->msec[msec]) {
                t->max = t->msec[msec];
            }
        } else if (hsec < 10) {
            t->halfsec[hsec]++;
            if (t->max < t->halfsec[hsec]) {
                t->max = t->halfsec[hsec];
            }
        } else {
            t->wayout++;
            if (t->max < t->wayout) {
                t->max = t->wayout;
            }
        }
    }
};

static void analyze(const std::string& dir,
                    const std::list<uint8_t>& opcodes,
                    timings_t aggregated[0x100]) {
    std::string name = CouchbaseDirectoryUtilities::basename(dir);
    std::replace(name.begin(), name.end(), '#', ':');
    std::cout << "Decoding stream " << name << std::endl;

    try {
        MemcachedStream stream(dir);
        if (opcodes.empty()) {
            stream.dump();
        } else {
            stream.dump(opcodes);
        }
        stream.aggregate(aggregated);
    } catch (std::runtime_error& e) {
        std::cerr << e.what() << std::endl;
    }
}

int main(int argc, char** argv) {
    int cmd;
    std::list<uint8_t> opcodes;
    bool aggregate = false;

    while ((cmd = getopt(argc, argv, "adc:")) != -1) {
        switch (cmd) {
        case 'a':
            aggregate = true;
            break;
        case 'c':
            opcodes.push_back(memcached_text_2_opcode(optarg));
            break;
        case 'd':
            packetdump = true;
            break;
        default:
            exit(1);
        }
    }

    if (optind == argc) {
        std::cerr << "Usage: cbtrace [options] file" << std::endl;
        exit(EXIT_FAILURE);
    }

    for (; optind < argc; ++optind) {
        std::string cachefile = argv[optind];
        cachefile += ".cache";

        if (access(cachefile.c_str(), F_OK) == -1) {
            std::cout << "Parsing file" << std::endl;

            try {
                createCache(cachefile, argv[optind]);
            } catch (std::runtime_error& e) {
                std::cerr << e.what() << std::endl;
                if (!CouchbaseDirectoryUtilities::rmrf(cachefile)) {
                    if (access(cachefile.c_str(), F_OK) != -1) {
                        std::cerr << "You should manually nuke " << cachefile
                        << std::endl;
                    }
                }
                return EXIT_FAILURE;
            }
        } else {
            std::cout << "Using cache in " << cachefile.c_str() << std::endl;
        }

        std::vector<std::string> streams;
        using namespace CouchbaseDirectoryUtilities;
        streams = findFilesWithPrefix(cachefile, "");

        timings_t aggregated[0x100];
        for (std::vector<std::string>::iterator iter = streams.begin();
             iter != streams.end();
             ++iter) {
            analyze(*iter, opcodes, aggregated);
        }

        if (aggregate) {
            std::cout << "Aggregated results:" << std::endl;
            // *Dump the histograms
            if (opcodes.empty()) {
                for (int ii = 0; ii < 0x100; ++ii) {
                    if (aggregated[ii].total) {
                        const char* name = memcached_opcode_2_text(
                            (uint8_t) ii);
                        if (name) {
                            fprintf(stdout, "Dump of %s (%u)\n",
                                    name, aggregated[ii].total);
                        } else {
                            fprintf(stdout, "Dump of %02x (%u)\n",
                                    (uint8_t) ii, aggregated[ii].total);
                        }
                        histogram(aggregated + ii);
                    }
                }
            } else {
                std::list<uint8_t>::const_iterator iter;
                for (iter = opcodes.begin(); iter != opcodes.end(); ++iter) {
                    uint8_t ii = *iter;
                    if (aggregated[ii].total) {
                        const char* name = memcached_opcode_2_text(ii);
                        if (name) {
                            fprintf(stdout, "Dump of %s (%u)\n",
                                    name, aggregated[ii].total);
                        } else {
                            fprintf(stdout, "Dump of %02x (%u)\n",
                                    ii, aggregated[ii].total);
                        }
                        histogram(aggregated + ii);
                    }
                }
            }
        }
    }

    return EXIT_SUCCESS;
}
