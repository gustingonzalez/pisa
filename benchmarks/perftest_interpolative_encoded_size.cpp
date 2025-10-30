#include <algorithm>
#include <chrono>
#include <cstdint>
#include <iomanip>
#include <iostream>
#include <limits>
#include <numeric>
#include <random>
#include <string>
#include <vector>

#include "codec/block_codecs.hpp"
#include "codec/interpolative.hpp"

using namespace pisa;
using clock_type = std::chrono::steady_clock;

auto benchmark(
    std::vector<std::uint32_t> const& values,
    std::uint32_t sum_of_values,
    std::string const& description,
    int iterations
) -> std::tuple<double, double, double> {
    std::vector<std::uint8_t> encoded;

    auto start = clock_type::now();
    volatile std::size_t encoded_size_sink = 0;
    for (int i = 0; i < iterations; ++i) {
        encoded.clear();
        interpolative_block::encode(values.data(), sum_of_values, values.size(), encoded);
        encoded_size_sink = encoded.size();
    }
    auto const encode_ns =
        std::chrono::duration_cast<std::chrono::nanoseconds>(clock_type::now() - start).count()
        / static_cast<double>(iterations);

    volatile std::size_t computed_size_sink = 0;
    start = clock_type::now();
    for (int i = 0; i < iterations; ++i) {
        computed_size_sink =
            interpolative_block::compute_encoded_size(values.data(), sum_of_values, values.size());
    }
    auto const compute_ns =
        std::chrono::duration_cast<std::chrono::nanoseconds>(clock_type::now() - start).count()
        / static_cast<double>(iterations);

    auto const actual_size = static_cast<std::size_t>(encoded_size_sink);
    auto const predicted_size = static_cast<std::size_t>(computed_size_sink);

    if (actual_size != predicted_size) {
        std::cerr << "ERROR: Size mismatch for " << description << " predicted=" << predicted_size
                  << " actual=" << actual_size << '\n';
        std::exit(1);
    }

    auto const speedup = (compute_ns == 0.0) ? 0.0 : encode_ns / compute_ns;

    std::cout << std::left << std::setw(55) << description << std::setw(12) << std::fixed
              << std::setprecision(1) << encode_ns << std::setw(14) << compute_ns << std::setw(10)
              << std::setprecision(2) << speedup << "  " << predicted_size << '\n';

    return std::make_tuple(encode_ns, compute_ns, speedup);
}

int main() {
    // Benchmark operation repetitions (cache warmup + noise smoothing).
    constexpr uint32_t warmup_iterations = 999;

    // Number of test cases to generate.
    constexpr uint32_t num_of_lists = 128;

    constexpr uint32_t unknown_sum_of_values = std::numeric_limits<uint32_t>::max();

    std::cout << std::left << std::setw(55) << "Description" << std::setw(12) << "Encode (ns)"
              << std::setw(14) << "Compute (ns)" << std::setw(10) << "Speedup" << "  Size (bytes)"
              << '\n';
    std::cout << std::string(105, '-') << '\n';

    double total_encode_time = 0.0;
    double total_compute_time = 0.0;
    double total_speedup = 0.0;
    std::size_t run_count = 0;

    // Random number generators (note: fixed seed for reproducibility).
    std::mt19937 rng(1);
    std::uniform_int_distribution<std::size_t> random_length(1, interpolative_block::block_size);
    std::uniform_int_distribution<std::uint32_t> random_value(
        0, std::numeric_limits<std::uint32_t>::max()
    );

    for (int i = 0; i < num_of_lists; ++i) {
        std::size_t length = random_length(rng);
        std::vector<std::uint32_t> values(length);
        for (auto& val: values) {
            val = random_value(rng);
        }
        // Sort values as expected by interpolative.
        std::sort(values.begin(), values.end());
        auto const sum = std::accumulate(values.begin(), values.end(), 0U);

        // Test with known `sum_of_values`, and with unknown `sum_of_values`.
        for (auto const& [sum_to_use, label]: std::vector<std::pair<uint32_t, std::string>>{
                 {sum, "with `sum_of_values`"}, {unknown_sum_of_values, ""}
             }) {
            std::string full_name = "List of " + std::to_string(values.size()) + " elements"
                + (label.empty() ? "" : " (" + label + ")");
            auto const benchmark_result = benchmark(values, sum_to_use, full_name, warmup_iterations);
            total_encode_time += std::get<0>(benchmark_result);
            total_compute_time += std::get<1>(benchmark_result);
            total_speedup += std::get<2>(benchmark_result);
            ++run_count;
        }
    }

    if (run_count != 0) {
        auto const avg_encode_time = total_encode_time / run_count;
        auto const avg_compute_time = total_compute_time / run_count;
        auto const avg_speedup = total_speedup / run_count;

        std::cout << std::string(105, '-') << '\n';
        std::cout << "Average encode time (ns): " << std::fixed << std::setprecision(1)
                  << avg_encode_time << '\n';
        std::cout << "Average compute time (ns): " << std::fixed << std::setprecision(1)
                  << avg_compute_time << '\n';
        std::cout << "Average speedup: " << std::fixed << std::setprecision(2) << avg_speedup
                  << '\n';
    }

    return 0;
}
