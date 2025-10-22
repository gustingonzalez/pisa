#include <algorithm>
#include <numeric>
#include <optional>
#include <thread>

#include "boost/algorithm/string/predicate.hpp"
#include "spdlog/spdlog.h"
#include <string>
#include <zlib.h>
#include <sstream>
#include <stdexcept>

#include "mappable/mapper.hpp"

#include "index_types.hpp"
#include "multi_freq_index.hpp"
#include "util/index_build_utils.hpp"
#include "util/progress.hpp"
#include "util/util.hpp"
#include "util/verify_collection.hpp" // XXX move to index_build_utils
#include "multicompression/stats.hpp"

#include <CLI/CLI.hpp>

using namespace pisa;

template <typename InputCollection, typename CollectionType>
void create_collection(InputCollection const &input,
                       pisa::global_parameters const &params,
                       const std::optional<std::string> &output_filename,
                       bool stats,
                       bool check)
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

        gzFile docs_stats_gz = nullptr;
        gzFile freqs_stats_gz = nullptr;
        std::ostringstream docs_csv_buf;
        std::ostringstream freqs_csv_buf;
        auto open_gzip_file = [](std::string const& path) -> gzFile {
            gzFile file = gzopen(path.c_str(), "wb");
            if (file == nullptr) {
                throw std::runtime_error("Unable to open gzip file: " + path);
            }
            return file;
        };
        auto close_gzip_file = [](gzFile file) {
            if (file == nullptr) {
                return;
            }
            int rc = gzclose(file);
            if (rc != Z_OK) {
                throw std::runtime_error("Failed to close gzip file");
            }
        };
        auto write_gzip = [](gzFile file, std::string const& payload) {
            if (payload.empty()) {
                return;
            }
            int written = gzwrite(file, payload.data(), static_cast<unsigned int>(payload.size()));
            if (written == 0) {
                int errnum = 0;
                const char* msg = gzerror(file, &errnum);
                throw std::runtime_error(
                    std::string("Failed to write gzip data: ") + (msg ? msg : "unknown error")
                );
            }
        };
        if (stats) {
            docs_stats_gz = open_gzip_file(output_filename.value() + ".stats.docs.gz");
            docs_csv_buf.str("");
            docs_csv_buf.clear();
            pisa::MulticompressionStatsManager::write_headers(docs_csv_buf);
            write_gzip(docs_stats_gz, docs_csv_buf.str());

            freqs_stats_gz = open_gzip_file(output_filename.value() + ".stats.freqs.gz");
            freqs_csv_buf.str("");
            freqs_csv_buf.clear();
            pisa::MulticompressionStatsManager::write_headers(freqs_csv_buf);
            write_gzip(freqs_stats_gz, freqs_csv_buf.str());
        }

        auto plist_id = 0;
        for (auto const &plist : input) {
            uint64_t freqs_sum;
            size = plist.docs.size();
            freqs_sum =
                std::accumulate(plist.freqs.begin(), plist.freqs.begin() + size, uint64_t(0));
            auto [dstats, fstats] =
                builder.add_posting_list(size, plist.docs.begin(), plist.freqs.begin(), freqs_sum);

            if (stats) {
                docs_csv_buf.str("");
                docs_csv_buf.clear();
                pisa::MulticompressionStatsManager::write_stats(
                    plist_id, size, dstats, docs_csv_buf
                );
                write_gzip(docs_stats_gz, docs_csv_buf.str());

                freqs_csv_buf.str("");
                freqs_csv_buf.clear();
                pisa::MulticompressionStatsManager::write_stats(
                    plist_id, size, fstats, freqs_csv_buf
                );
                write_gzip(freqs_stats_gz, freqs_csv_buf.str());
                plist_id++;
            }

            progress.update(1);
            postings += size;
        }
        if (stats) {
            close_gzip_file(docs_stats_gz);
            close_gzip_file(freqs_stats_gz);
        }
    }

    CollectionType coll;
    builder.build(coll);
    double elapsed_secs = (get_time_usecs() - tick) / 1000000;
    spdlog::info("{} collection built in {} seconds", seq_type, elapsed_secs);

    stats_line()("type", seq_type)("worker_threads", std::thread::hardware_concurrency())(
        "construction_time", elapsed_secs);

    dump_stats(coll, seq_type, postings);
    // dump_index_specific_stats(coll, seq_type);

    if (output_filename) {
        mapper::freeze(coll, (*output_filename).c_str());
        if (check) {
            verify_collection<binary_freq_collection, CollectionType>(
                input, (*output_filename).c_str()
            );
        }
    }
}

int main(int argc, char **argv)
{
    using namespace pisa;
    std::string type;
    std::string input_basename;
    std::optional<std::string> output_filename;
    bool stats = false;
    bool check = false;

    CLI::App app{"create_multi_freq_index - a tool for creating a multicompressed index."};
    app.add_option("-c,--collection", input_basename, "Collection basename")->required();
    app.add_option("-o,--output", output_filename, "Output filename")->required();
    app.add_flag("--stats", stats, "Generate stats per posting list chunk");
    app.add_flag("--check", check, "Check the correctness of the index");
    CLI11_PARSE(app, argc, argv);

    binary_freq_collection input(input_basename.c_str());
    pisa::global_parameters params;
    params.log_partition_size = params.log_partition_size;

    using coll_type = multi_freq_index<false>;
    create_collection<binary_freq_collection, coll_type>(
        input, params, output_filename, stats, check);

    return 0;
}
