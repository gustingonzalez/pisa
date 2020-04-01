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
    test_block_codec<pisa::interpolative_block>();
    test_block_codec<pisa::qmx_block>();
    test_block_codec<pisa::varintgb_block>();
    test_block_codec<pisa::simple8b_block>();
    test_block_codec<pisa::simple16_block>();

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