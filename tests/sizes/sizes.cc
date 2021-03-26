#include <daemon/connection.h>
#include <daemon/cookie.h>
#include <daemon/front_end_thread.h>
#include <daemon/settings.h>
#include <daemon/stats.h>
#include <cstdio>

static void display(const char *name, size_t size) {
    printf("%-20s\t%d\n", name, (int)size);
}

static unsigned int count_used_opcodes() {
    unsigned int used_opcodes = 0;
    for (uint8_t opcode = 0; opcode < 255; opcode++) {
        try {
            to_string(cb::mcbp::ClientOpcode(opcode));
            used_opcodes++;
        } catch (const std::exception&) {
        }
    }
    return used_opcodes;
}

static void display_used_opcodes() {
    printf("\nClientOpcode map:     (X = Used, . = Free)\n\n");
    printf("   0123456789abcdef");
    for (unsigned int opcode = 0; opcode < 256; opcode++) {
        if (opcode % 16 == 0) {
            printf("\n%02x ", opcode & ~0xf);
        }
        try {
            to_string(cb::mcbp::ClientOpcode(opcode));
            putchar('X');
        } catch (const std::exception&) {
            putchar('.');
        }
    }
    putchar('\n');
}

int main() {
    display("Thread stats", sizeof(struct thread_stats));
    display("Global stats", sizeof(struct stats));
    display("Settings", sizeof(Settings));
    display("Libevent thread", sizeof(FrontEndThread));
    display("Connection", sizeof(Connection));
    display("Cookie", sizeof(Cookie));

    printf("----------------------------------------\n");
    printf("Binary protocol opcodes used\t%u / 256\n", count_used_opcodes());
    display_used_opcodes();

    return 0;
}
