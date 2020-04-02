#include <algorithm>
#include <fstream>
#include <iostream>
#include <numeric>
#include <optional>
#include <thread>

#include "boost/algorithm/string/predicate.hpp"
#include "spdlog/spdlog.h"

#include "mappable/mapper.hpp"

#include "configuration.hpp"
#include "index_types.hpp"
#include "multi_freq_index.hpp"
#include "util/index_build_utils.hpp"
#include "util/util.hpp"
#include "util/verify_collection.hpp" // XXX move to index_build_utils
#include "multicompression/stats.hpp"

#include "CLI/CLI.hpp"

using namespace pisa;

template <typename InputCollection, typename CollectionType>
void create_collection(InputCollection const &input,
                       pisa::global_parameters const &params,
                       const std::optional<std::string> &output_filename,
                       bool stats = false,
                       bool check = true)
{
    using namespace pisa;
    std::string const seq_type = "Multicompression";
    spdlog::info("Processing {} documents", input.num_docs());
    double tick = get_time_usecs();

    typename CollectionType::builder builder(input.num_docs(), params);
    uint64_t size = 0;
    size_t postings = 0;
    {
        pisa::progress progress("Create index", input.size());
        ofstream odstats{output_filename.value() + ".stats.docs"};
        ofstream ofstats{output_filename.value() + ".stats.freqs"};
        pisa::MulticompressionStatsManager::write_headers(odstats);
        pisa::MulticompressionStatsManager::write_headers(ofstats);

        auto plist_id = 0;
        for (auto const &plist : input) {
            uint64_t freqs_sum;
            size = plist.docs.size();
            freqs_sum =
                std::accumulate(plist.freqs.begin(), plist.freqs.begin() + size, uint64_t(0));
            auto [dstats, fstats] =
                builder.add_posting_list(size, plist.docs.begin(), plist.freqs.begin(), freqs_sum);

            if (stats) {
                pisa::MulticompressionStatsManager::write_stats(plist_id, size, dstats, odstats);
                pisa::MulticompressionStatsManager::write_stats(plist_id, size, fstats, ofstats);
                plist_id++;
            }

            progress.update(1);
            postings += size;
        }
        odstats.close();
        ofstats.close();
    }

    CollectionType coll;
    builder.build(coll);
    double elapsed_secs = (get_time_usecs() - tick) / 1000000;
    spdlog::info("{} collection built in {} seconds", seq_type, elapsed_secs);

    stats_line()("type", seq_type)("worker_threads", configuration::get().worker_threads)(
        "construction_time", elapsed_secs);

    dump_stats(coll, seq_type, postings);
    // dump_index_specific_stats(coll, seq_type);

    if (output_filename) {
        mapper::freeze(coll, (*output_filename).c_str());
        if (check) {
            verify_collection<binary_freq_collection, CollectionType>(input,
                                                                      (*output_filename).c_str());
        }
    }
}

int main(int argc, char **argv)
{
    using namespace pisa;
    std::string type;
    std::string input_basename;
    std::optional<std::string> output_filename;
    bool check = false;

    CLI::App app{"create_multi_freq_index - a tool for creating a multicompressed index."};
    app.add_option("-c,--collection", input_basename, "Collection basename")->required();
    app.add_option("-o,--output", output_filename, "Output filename")->required();
    app.add_flag("--check", check, "Check the correctness of the index");
    CLI11_PARSE(app, argc, argv);

    binary_freq_collection input(input_basename.c_str());
    pisa::global_parameters params;
    params.log_partition_size = configuration::get().log_partition_size;

    using coll_type = multi_freq_index<false>;
    create_collection<binary_freq_collection, coll_type>(
        input, params, output_filename, true, check);

    return 0;
}