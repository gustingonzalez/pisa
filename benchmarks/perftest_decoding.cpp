#include "mappable/mapper.hpp"
#include "mio/mmap.hpp"
#include "spdlog/spdlog.h"

#include "index_types.hpp"
#include "util/do_not_optimize_away.hpp"

using pisa::do_not_optimize_away;
using pisa::get_time_usecs;

#include "index_types.hpp"
#include "multi_freq_index.hpp"
#include <iostream>

template <typename IndexType, bool decode_freqs>
double perftest(IndexType const &index)
{
    auto start = get_time_usecs();
    for (int i = 0; i < index.size(); i++) {
        auto plist = index[i];
        // Reads elements of each posting list.
        for (size_t i = 0; i < plist.size(); ++i) {
            // Puts pointer on next posting.
            plist.next();

            // Loads docid and freq (if required) of current posting. On the other hand,
            // do_not_optimize_away() is used to avoid compiler optimizations. Otherwise,
            // the compiler may decide not to execute the following sentences because the
            // result isn't used.
            do_not_optimize_away(plist.docid());
            if (decode_freqs) {
                do_not_optimize_away(plist.freq());
            }
        }
    }
    double elapsed = get_time_usecs() - start;
    return double(elapsed / 1000); // ms.
}

template <typename IndexType>
void perftest(const char *index_filename)
{
    IndexType index;
    mio::mmap_source m(index_filename);
    pisa::mapper::map(index, m, pisa::mapper::map_flags::warmup);

    // Executes test decoding only docids (first test is warm-up).
    spdlog::info("Decoding posting lists (only docs)...");
    perftest<IndexType, false>(index); // Warm-up
    double t1 = perftest<IndexType, false>(index);
    double t2 = perftest<IndexType, false>(index);
    double avg = (t1 + t2) / 2;
    spdlog::info("Decoding (only docs) average: {} ms.", avg);

    // Executes test decoding docids and freqs (first test is warm-up).
    spdlog::info("Decoding posting lists (with docs)...");
    perftest<IndexType, true>(index); // Warm-up
    t1 = perftest<IndexType, true>(index);
    t2 = perftest<IndexType, true>(index);
    avg = (t1 + t2) / 2;
    spdlog::info("Decoding (with freqs) average: {} ms.", avg);
}

int main(int argc, const char **argv)
{

    using namespace pisa;

    if (argc != 3) {
        std::cerr << "Usage: " << argv[0] << " <index filename> <index type>" << std::endl;
        return 1;
    }

    const char *index_filename = argv[1];
    const std::string type = argv[2];

    spdlog::info("Performing test of {} ({})", index_filename, type);
    if (type == "multi") {
        perftest<multi_freq_index<false>>(index_filename);
    } else if (type == "block_optpfor") {
        perftest<block_optpfor_index>(index_filename);
    } else if (type == "block_varintg8iu") {
        perftest<block_varintg8iu_index>(index_filename);
    } else if (type == "block_streamvbyte") {
        perftest<block_streamvbyte_index>(index_filename);
    } else if (type == "block_maskedvbyte") {
        perftest<block_maskedvbyte_index>(index_filename);
    } else if (type == "block_varintgb") {
        perftest<block_varintgb_index>(index_filename);
    } else if (type == "block_interpolative") {
        perftest<block_interpolative_index>(index_filename);
    } else if (type == "block_qmx") {
        perftest<block_qmx_index>(index_filename);
    } else if (type == "block_simple8b") {
        perftest<block_simple8b_index>(index_filename);
    } else if (type == "block_simple16") {
        perftest<block_simple8b_index>(index_filename);
    } else if (type == "block_simdbp") {
        perftest<block_simple8b_index>(index_filename);
    } else if (type == "block_mixed") {
        perftest<block_mixed_index>(index_filename);
    } else if (type == "ef_index") {
        perftest<ef_index>(index_filename);
    } else if (type == "single_index") {
        perftest<single_index>(index_filename);
    } else if (type == "pefuniform_index") {
        perftest<pefuniform_index>(index_filename);
    } else if (type == "pefopt_index") {
        perftest<pefopt_index>(index_filename);
    } else {
        spdlog::error("Unknown type {}", type);
        return 1;
    }
    return 0;
}