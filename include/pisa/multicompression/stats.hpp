#pragma once
#include <ostream>
#include <sstream>

namespace pisa {
struct ChunkStatistic {
    uint8_t Codec;
    uint32_t ChunkSize;
    uint32_t CompressedSize;
    uint32_t ZeroCount;
    uint32_t LesserThan8Count;
    uint32_t MinGap;
    uint32_t MinNum;
    uint32_t MaxGap;
    uint32_t MaxNum;
    float AvgDistanceGap;
    float AvgDistanceNum;

    ChunkStatistic(std::vector<uint32_t> &gaps,
                   uint32_t chunk_size,
                   uint8_t codec,
                   size_t compressed_size,
                   bool are_freqs)
    {
        Codec = codec;
        ChunkSize = chunk_size;
        CompressedSize = compressed_size;

        std::vector<uint32_t> numbers(ChunkSize);

        // Adds 1 to each gaps[n] with n >= 0, if the list are freqs.
        // Adds 1 to each gaps[n] with n > 0, if the list are docs.
        numbers[0] = are_freqs ? gaps[0] + 1 : gaps[0];
        for (auto i = 1; i < ChunkSize; ++i) {
            numbers[i] = numbers[i - 1] + gaps[i] + 1;
        }

        auto gaps_end = gaps.begin() + ChunkSize;
        ZeroCount = std::count(gaps.begin(), gaps_end, 0);
        LesserThan8Count = std::count_if(gaps.begin(), gaps_end, [](auto v) { return v < 8; });

        MinGap = *std::min_element(gaps.begin(), gaps_end);
        MinNum = numbers[0];

        MaxGap = *std::max_element(gaps.begin(), gaps_end);
        MaxNum = numbers[ChunkSize - 1];

        AvgDistanceNum = (float) MaxNum / ChunkSize;
        AvgDistanceGap = gaps[0];
        for (auto i = 1; i < ChunkSize; ++i) {
            int32_t distance = gaps[i - 1] - gaps[i];
            AvgDistanceGap += abs(distance);
        }
        AvgDistanceGap /= ChunkSize;
    }
};

class MulticompressionStatsManager {
   public:
    static void write_stats(uint32_t plist_id,
                            size_t plist_size,
                            std::vector<ChunkStatistic> &stats,
                            std::ostream &output)
    {
        output << std::fixed;

        std::stringstream ss;
        ss << std::fixed << std::setprecision(2);
        for (auto stat : stats) {
            ss << std::to_string(plist_id) << ",";
            ss << std::to_string(plist_size) << ",";
            ss << std::to_string(stat.Codec) << ",";
            ss << std::to_string(stat.ChunkSize) << ",";
            ss << std::to_string(stat.CompressedSize) << ",";
            ss << std::to_string(stat.MinGap) << ",";
            ss << std::to_string(stat.MinNum) << ",";
            ss << std::to_string(stat.MaxGap) << ",";
            ss << std::to_string(stat.MaxNum) << ",";
            ss << stat.AvgDistanceGap << ",";
            ss << stat.AvgDistanceNum << ",";
            ss << std::to_string(stat.ZeroCount) << ",";
            ss << std::to_string(stat.LesserThan8Count) << "\n";
        }
        output << ss.str();
    }

    static void write_headers(std::ostream &output)
    {
        output << "PostingListId,PostingListSize,Codec,ChunkSize,";
        output << "CompressedSize,MinGap,MinNum,MaxGap,MaxNum,";
        output << "AvgDistanceGap,AvgDistanceNum,ZeroCount,";
        output << "LesserThan8Count\n";
    }
};
} // namespace pisa