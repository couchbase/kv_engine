#ifndef TESTS_H
#define TESTS_H 1

#include "base-test.hh"

namespace kvtest {

    /**
     * A test to run.
     */
    class Test : public Assertions {
    public:

        virtual ~Test() {}

        /**
         * Run the test.
         */
        virtual bool run(KVStore *tut) = 0;

        /**
         * Name of the test.
         */
        virtual std::string name() = 0;

        /**
         * Tests print out their names.
         */
        friend std::ostream& operator<<(std::ostream& s, Test &t) {
            return s << t.name();
        }

    };

}

/**
 * A really basic protocol compliance test.
 */
class TestTest : public kvtest::Test {
public:
    virtual ~TestTest() {}
    bool run(kvtest::KVStore *tut);
    std::string name() { return "test test"; }
};

/**
 * A test of write efficiency.
 */
class WriteTest : public kvtest::Test {
public:
    virtual ~WriteTest() {}
    bool run(kvtest::KVStore *tut);
    std::string name() { return "write test"; }
};

/**
 * A long-running endurance test.
 */
class EnduranceTest : public kvtest::Test {
public:
    virtual ~EnduranceTest() {}
    bool run(kvtest::KVStore *tut);
    std::string name() { return "endurance test"; }
};

#endif /* TESTS_H */
