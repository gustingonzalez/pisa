#define CATCH_CONFIG_MAIN
#include "catch2/catch.hpp"
#include "pisa/greedy_partition.hpp"

TEST_CASE("Greedy partition") {
    uint32_t list[] = {1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 10089};
    std::vector<uint32_t> partitions = pisa::GreedyPartition::compute(list, 17, 8);
    REQUIRE(std::vector<uint32_t>({16, 1}) == partitions);
}

TEST_CASE("Greedy partition 2") {
    uint32_t list[] = {1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 1000, 10089};
    std::vector<uint32_t> partitions = pisa::GreedyPartition::compute(list, 12, 8);
    REQUIRE(std::vector<uint32_t>({8, 4}) == partitions);
}

TEST_CASE("Greedy partition 3") {
    uint32_t list[] = {1, 2, 3, 4};
    std::vector<uint32_t> partitions = pisa::GreedyPartition::compute(list, 4, 8);
    REQUIRE(std::vector<uint32_t>({4}) == partitions);
}

TEST_CASE("Greedy partition 4") {
    uint32_t list[] = {1, 2, 3, 4, 8, 9, 10, 15, 16, 17, 19, 23, 28, 39, 40, 50, 58};
    std::vector<uint32_t> partitions = pisa::GreedyPartition::compute(list, 17, 8);
    REQUIRE(std::vector<uint32_t>({8, 9}) == partitions);
}

TEST_CASE("Greedy partition 5") {
    uint32_t list[] = {1, 2, 3, 4, 8, 9, 10, 15, 16, 17, 19, 23};
    std::vector<uint32_t> partitions = pisa::GreedyPartition::compute(list, 12, 8);
    REQUIRE(std::vector<uint32_t>({12}) == partitions);
}