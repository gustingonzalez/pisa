#define CATCH_CONFIG_MAIN
#include "catch2/catch.hpp"
#include "multicompression/stats.hpp"

void test_docs_stat_with_defined_gaps(uint8_t codec,
                                      std::vector<uint32_t> gaps,
                                      uint32_t chunk_size,
                                      uint32_t compressed_size,
                                      pisa::ChunkStatistic stat)
{
    REQUIRE(codec == stat.Codec);
    REQUIRE(chunk_size == stat.ChunkSize);
    REQUIRE(compressed_size == stat.CompressedSize);
    REQUIRE(1 == stat.MinNum);
    REQUIRE(0 == stat.MinGap);
    REQUIRE(2 == stat.MaxGap);
    REQUIRE(8 == stat.MaxNum);
    REQUIRE(1 == (uint32_t)stat.AvgDistanceGap);
    REQUIRE(2 == (uint32_t)stat.AvgDistanceNum);
    REQUIRE(1 == stat.ZeroCount);
    REQUIRE(4 == stat.LesserThan8Count);
}

void test_freqs_stat_with_defined_gaps(uint8_t codec,
                                       std::vector<uint32_t> gaps,
                                       uint32_t chunk_size,
                                       uint32_t compressed_size,
                                       pisa::ChunkStatistic stat)
{
    REQUIRE(codec == stat.Codec);
    REQUIRE(chunk_size == stat.ChunkSize);
    REQUIRE(compressed_size == stat.CompressedSize);
    REQUIRE(1 == stat.MinNum);
    REQUIRE(0 == stat.MinGap);
    REQUIRE(2 == stat.MaxGap);
    REQUIRE(8 == stat.MaxNum);
    REQUIRE(5 == (uint32_t)(stat.AvgDistanceGap * 10)); // 0.5 == 0.5
    REQUIRE(2 == (uint32_t)stat.AvgDistanceNum);
    REQUIRE(2 == stat.ZeroCount);
    REQUIRE(4 == stat.LesserThan8Count);
}

TEST_CASE("Create docs chunk statistic with defined gaps and chunk size equal to gaps size")
{
    std::vector<uint32_t> gaps({1, 0, 2, 2}); // {1, 2, 5, 8}
    auto codec = 1;
    auto compressed_size = 8;
    pisa::ChunkStatistic stat(gaps, gaps.size(), codec, compressed_size, false);
    test_docs_stat_with_defined_gaps(codec, gaps, gaps.size(), compressed_size, stat);
}

TEST_CASE("Create docs chunk statistic with defined gaps and chunk size lesser than gaps size")
{
    auto chunk_size = 4;
    std::vector<uint32_t> gaps({1, 0, 2, 2, 0, 0, 0}); // {1, 2, 5, 8}
    auto codec = 1;
    auto compressed_size = 8;
    pisa::ChunkStatistic stat(gaps, chunk_size, codec, compressed_size, false);
    test_docs_stat_with_defined_gaps(codec, gaps, chunk_size, compressed_size, stat);
}

TEST_CASE("Create freqs chunk statistic with defined gaps and chunk size equal to gaps size")
{
    std::vector<uint32_t> gaps({0, 0, 2, 2}); // {1, 2, 5, 8}
    auto codec = (uint8_t)rand();
    auto compressed_size = (size_t)rand();
    pisa::ChunkStatistic stat(gaps, gaps.size(), codec, compressed_size, true);
    test_freqs_stat_with_defined_gaps(codec, gaps, gaps.size(), compressed_size, stat);
}

TEST_CASE("Create freqs chunk statistic with defined gaps and chunk size lesser than gaps size")
{
    auto chunk_size = 4;
    std::vector<uint32_t> gaps({0, 0, 2, 2, 0, 0, 0}); // {1, 2, 5, 8}
    auto codec = (uint8_t)rand();
    auto compressed_size = (size_t)rand();
    pisa::ChunkStatistic stat(gaps, chunk_size, codec, compressed_size, true);
    test_freqs_stat_with_defined_gaps(codec, gaps, chunk_size, compressed_size, stat);
}