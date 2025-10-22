#pragma once
#include <cstddef>
#include <cstdint>
#include <ostream>
#include <vector>

namespace pisa {
struct ChunkStatistic {
    uint8_t Codec{};
    uint32_t ChunkSize{};
    size_t CompressedSize{};
    bool AreFreqs{};
    std::vector<uint32_t> Gaps;

    ChunkStatistic(std::vector<uint32_t> &gaps,
                   uint32_t chunk_size,
                   uint8_t codec,
                   size_t compressed_size,
                   bool are_freqs)
        : Codec(codec),
          ChunkSize(chunk_size),
          CompressedSize(compressed_size),
          AreFreqs(are_freqs),
          Gaps(gaps.begin(), gaps.begin() + chunk_size)
    {}
};

class MulticompressionStatsManager {
   public:
    static void write_stats(uint32_t plist_id,
                            size_t plist_size,
                            std::vector<ChunkStatistic> &stats,
                            std::ostream &output)
    {
        for (size_t chunk_idx = 0; chunk_idx < stats.size(); ++chunk_idx) {
            auto const &stat = stats[chunk_idx];

            output << plist_id << ","
                   << plist_size << ","
                   << chunk_idx << ","
                   << stat.ChunkSize << ","
                   // << (stat.AreFreqs ? "F" : "D") << ","
                   << static_cast<uint32_t>(stat.Codec) << ","
                   << static_cast<uint64_t>(stat.CompressedSize) << ",";

            for (size_t i = 0; i < stat.Gaps.size(); ++i) {
                if (i != 0) {
                    output << " ";
                }
                output << stat.Gaps[i];
            }
            output << "\n";
        }
    }


    /**
     * CSV fields:
     * - posting_list_id: Identifier of the posting list.
     * - posting_list_size: Number of elements in the posting list.
     * - chunk_index: Zero-based chunk index within the posting list.
     * - codec: Compression codec identifier applied to the chunk.
     * - chunk_size: Number of values in the chunk.
     * - compressed_size: Chunk size in bytes after compression.
     * - values: Payload (for docs: gaps-1, for freqs: values-1).
     */
    static void write_headers(std::ostream &output)
    {
        output
            << "posting_list_id,"
            << "posting_list_size,"
            << "chunk_index,"
            << "chunk_size,"
            << "codec,"
            << "compressed_size,"
            << "values\n";
    }
};
} // namespace pisa
