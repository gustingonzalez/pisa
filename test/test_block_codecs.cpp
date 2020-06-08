#define CATCH_CONFIG_MAIN
#include "catch2/catch.hpp"

#include <cstdlib>
#include <vector>

#include "codec/block_codecs.hpp"
#include "codec/maskedvbyte.hpp"
#include "codec/qmx.hpp"
#include "codec/simdbp.hpp"
#include "codec/simple16.hpp"
#include "codec/simple8b.hpp"
#include "codec/streamvbyte.hpp"
#include "codec/varintgb.hpp"

#include "test_common.hpp"

template <typename BlockCodec>
void test_block_codec(std::vector<size_t> sizes = {
                          1, 16, BlockCodec::block_size - 1, BlockCodec::block_size})
{
    for (auto size : sizes) {
        std::vector<uint32_t> values(size);
        std::generate(values.begin(), values.end(), []() { return (uint32_t)rand() % (1 << 12); });

        for (size_t tcase = 0; tcase < 2; ++tcase) {
            // test both undefined and given sum_of_values
            uint32_t sum_of_values(-1);
            if (tcase == 1) {
                sum_of_values = std::accumulate(values.begin(), values.end(), 0);
            }
            std::vector<uint8_t> encoded;
            BlockCodec::encode(values.data(), sum_of_values, values.size(), encoded);
            std::vector<uint32_t> decoded(values.size());
            uint8_t const *out =
                BlockCodec::decode(encoded.data(), decoded.data(), sum_of_values, values.size());
            REQUIRE(encoded.size() == out - encoded.data());
            REQUIRE(std::equal(values.begin(), values.end(), decoded.begin()));
        }
    }
}

TEST_CASE("block_codecs")
{
    test_block_codec<pisa::streamvbyte_block>();
    test_block_codec<pisa::maskedvbyte_block>();
    test_block_codec<pisa::qmx_block>();
    test_block_codec<pisa::varintgb_block>();
    test_block_codec<pisa::simple8b_block>();
    test_block_codec<pisa::simple16_block>();

    // The minimum list size required for bic (in multicompression) is 2.
    test_block_codec<pisa::interpolative_block>(
        {2, 16, pisa::varint_G8IU_block::block_size - 1, pisa::varint_G8IU_block::block_size});

    // SIMDBP and PFD implementations works with chunks equals to block size.
    test_block_codec<pisa::simdbp_block>({pisa::simdbp_block::block_size});
    test_block_codec<pisa::optpfor_block>({pisa::optpfor_block::block_size});

    // The minimum list size required for varintG8IU implementation is 8.
    test_block_codec<pisa::varint_G8IU_block>(
        {8, 16, pisa::varint_G8IU_block::block_size - 1, pisa::varint_G8IU_block::block_size});
}

TEST_CASE("tight_variable_byte - single/next encode/decode")
{
    for (int i = 0; i < 8; i++) {
        uint32_t values[2] = {(uint32_t)rand(), (uint32_t)rand()};
        vector<uint8_t> encoded;

        pisa::TightVariableByte::encode_single(values[0], encoded);
        pisa::TightVariableByte::encode_single(values[1], encoded);

        uint8_t const *encpointer = encoded.data();
        uint32_t val;
        encpointer = pisa::TightVariableByte::next(encpointer, val);
        REQUIRE(val == values[0]);
        pisa::TightVariableByte::next(encpointer, val);
        REQUIRE(val == values[1]);
    }
}

void test_all_ones_block(bool test_docs)
{
    uint32_t n = pisa::all_ones_block::block_size;
    std::vector<uint32_t> values(n, 0);
    values[0] = test_docs * rand();
    uint32_t sum_of_values = test_docs ? values[0] : -1;

    bool is_encodable = pisa::all_ones_block::is_encodable(values.data(), sum_of_values, n);
    REQUIRE(is_encodable == true);

    std::vector<uint8_t> encoded; // Dummy (not used actually).
    std::vector<uint32_t> decoded(n);

    pisa::all_ones_block::decode(encoded.data(), decoded.data(), sum_of_values, n);
    REQUIRE(values == decoded);
}

TEST_CASE("all_ones_block - decode documents") { test_all_ones_block(true); }

TEST_CASE("all_ones_block - decode frequencies") { test_all_ones_block(false); }

TEST_CASE("all_ones_block - is encodable should return false")
{
    uint32_t n = pisa::all_ones_block::block_size;
    std::vector<uint32_t> values(n, 0);
    values[1] = 2 + rand() % (1 << 12);
    REQUIRE(pisa::all_ones_block::is_encodable(values.data(), values[0], n) == false);
}

void test_compute_exceptions(bool test_docs)
{
    uint32_t n = pisa::all_ones_block::block_size;
    std::vector<uint32_t> values(n, 0);
    uint32_t exception_count = 0;
    uint32_t expected_exceptions_positions[n];
    uint32_t expected_exceptions[n];
    int32_t last_exception_pos = test_docs ? 0 : -1;

    // Adds 'n / 2' exceptions.
    for (int i = test_docs; i < n; i += 2) {
        // Generates random value.
        uint32_t value = 1 + (rand() % (1 << 12));
        values[i] = value;

        // For documents, the value of the first integer no matters.
        uint32_t gap = i - last_exception_pos - 1;
        expected_exceptions_positions[exception_count] = gap;
        expected_exceptions[exception_count] = value - 1;
        exception_count++;
        last_exception_pos = i;
    }

    // Expected exceptions.
    std::vector<uint32_t> expected;
    expected.insert(expected.end(),
                    expected_exceptions_positions,
                    expected_exceptions_positions + exception_count);
    expected.insert(expected.end(), expected_exceptions, expected_exceptions + exception_count);

    // Computed exceptions.
    uint32_t sum_of_values = -1;
    if (test_docs) {
        sum_of_values = std::accumulate(values.begin(), values.end(), 0);
    }
    uint32_t computed_exception_count = pisa::many_ones_block::count_exceptions(values.data(), sum_of_values, n);
    auto computed = pisa::many_ones_block::compute_exceptions(values.data(), sum_of_values, n);

    REQUIRE(exception_count == computed_exception_count);
    REQUIRE(expected == computed);
}

void test_decode_many_ones_block(bool test_docs)
{
    uint32_t n = pisa::all_ones_block::block_size - 1;
    std::vector<uint32_t> values(n, 0);

    // Adds 'n / 2' exceptions.
    for (auto i = 0; i < n; i += 2) {
        values[i] = 1 + (rand() % (1 << 12));
    }

    // Verifies encode/decode.
    uint32_t sum_of_values = -1;
    if (test_docs) {
        sum_of_values = std::accumulate(values.begin(), values.end(), 0);
    }
    std::vector<uint8_t> encoded;
    std::vector<uint32_t> decoded(n);
    pisa::many_ones_block::encode(values.data(), sum_of_values, n, encoded);
    uint8_t const *out =
        pisa::many_ones_block::decode(encoded.data(), decoded.data(), sum_of_values, n);

    // Checks decoded values.
    REQUIRE(values == decoded);

    // Checks readed bytes.
    REQUIRE(encoded.size() == out - encoded.data());
}

TEST_CASE("many_ones_block - compute documents exceptions") { test_compute_exceptions(true); }

TEST_CASE("many_ones_block - compute frequencies exceptions") { test_compute_exceptions(false); }

TEST_CASE("many_ones_block - encode documents test") { test_decode_many_ones_block(true); }

TEST_CASE("many_ones_block - encode frequencies test") { test_decode_many_ones_block(false); }