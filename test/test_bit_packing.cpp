#include "multicompression/block_codecs.hpp"

#define CATCH_CONFIG_MAIN
#include "catch2/catch.hpp"

#include <limits>
#include <numeric>
#include <string>
#include <vector>

using namespace pisa;

struct TestCase {
    std::string name;
    std::vector<uint32_t> values{};

    TestCase(const std::string& name, const std::vector<uint32_t>& values)
        : name(name), values(values) {}
};

void check_roundtrip_and_encoded_size(TestCase const& test_case) {
    REQUIRE_FALSE(test_case.values.empty());
    auto const sum_of_values = std::accumulate(test_case.values.begin(), test_case.values.end(), 0U);

    INFO(test_case.name);

    // Verify computed size matches actual encoded size
    auto const expected_size = BitPackingBlockCodec::compute_encoded_size(
        test_case.values.data(), test_case.values.size()
    );
    std::vector<uint8_t> encoded;
    BitPackingBlockCodec::encode(test_case.values.data(), sum_of_values, test_case.values.size(), encoded);
    auto const actual_size = encoded.size();

    // Use `CHECK` instead of `REQUIRE` so both verifications run, even if
    // the first one fails (`REQUIRE` would abort the test case).
    CHECK(expected_size == actual_size);

    // Verify encode/decode roundtrip.
    std::vector<uint32_t> decoded(test_case.values.size());
    uint8_t const* out = BitPackingBlockCodec::decode(
        encoded.data(), decoded.data(), sum_of_values, test_case.values.size()
    );
    CHECK(test_case.values == decoded);
    CHECK(encoded.size() == static_cast<size_t>(out - encoded.data()));
}

TEST_CASE("Check computed encoded size, and make roundtrip", "[bitpacking]") {
    std::vector<TestCase> test_cases{
        {"8 ones", std::vector<uint32_t>(8, 1)},
        {"128 ones", std::vector<uint32_t>(128, 1)},
        {"16 zeros", std::vector<uint32_t>(16, 0)},
        {"63 zeros", std::vector<uint32_t>(63, 0)},
        {"127 constant values (value 9999)", std::vector<uint32_t>(127, 9999)},
        {"12 small values", {1, 2, 0, 1, 2, 1, 3, 0, 2, 1, 0, 1}},
        {"Increasing sequence [1..50]",
         [] {
             std::vector<uint32_t> values(50);
             std::iota(values.begin(), values.end(), 1);
             return values;
         }()},
        {"Single zero", {0}},
        {"Single one", {1}},
        {"Single max value", {0xFFFFFFFFu}},
        {"Pair [10, 90]", {10, 90}},
        // clang-format off
        {"Bit-width boundaries", {
            0, 1, 127, 128, 255, 256, // Byte boundaries
            32767, 32768, 65535, 65536, // Word boundaries
            2147483647u, 2147483648u, 4294967295u, // DWord boundaries
        }},
        // clang-format on
        {"3 24-bit large values", {0xFFFFFFu, 0xFFFFFEu, 0xFFFFFDu}},
        {"Max uint32_t", {0xFFFFFFFFu}},
        {"Exponential values",
         {1, 2, 4, 8, 16, 32, 64, 128, 256, 512, 1024, 2048, 4096, 8192, 16384, 32768}},
        {"Right-skewed distribution",
         [] {
             std::vector<uint32_t> values(20, 1);
             values.push_back(10000);
             values.push_back(50000);
             return values;
         }()},
        {"Fibonacci sequence", {1, 1, 2, 3, 5, 8, 13, 21, 34, 55, 89}},
        {"Powers of 2", {1, 2, 4, 8, 16, 32, 64, 128, 256, 512, 1024}},
        {"Example from encode comment", {5, 2, 7, 1, 3}},
        {"Alternating pattern", {1, 0, 1, 0, 1, 0, 1, 0, 1, 0}},
        {"Sparse values",
         [] {
             std::vector<uint32_t> values(100, 0);
             values[10] = 1000;
             values[50] = 2000;
             values[90] = 3000;
             return values;
         }()},
    };

    for (auto const& test_case: test_cases) {
        DYNAMIC_SECTION(test_case.name) {
            check_roundtrip_and_encoded_size(test_case);
        }
    }
}
