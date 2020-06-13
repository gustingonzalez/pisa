#pragma once

namespace pisa {
    struct GreedyPartition {

        static size_t compute_weight(uint32_t begin, uint32_t end, uint32_t block_size) {
            return ceil(log2((end - begin) / block_size));
        }

        template <typename Iterator>
        static auto compute(Iterator list, uint32_t n, int step) ->
        std::pair<std::vector<uint32_t>, std::vector<size_t>> {
            std::vector<uint32_t> partitions;
            std::vector<size_t> weights;
            uint32_t last_computed(-1);
            uint32_t list_size = 0;

            // begin-end window
            uint32_t begin = 0;
            uint32_t end = step < n ? step - 1 : n - 1;
            uint32_t block_size = end - begin + 1;
            uint32_t begin_element = list[0];
            size_t weight = compute_weight(begin_element, list[end], block_size);
            bool increment_window = false;
            
            uint32_t end_candidate;
            uint32_t block_size_cantidate;
            size_t weight_candidate;
            do {
                if (increment_window) {
                    // Updates end, weight and block size based on candidate.
                    end = end_candidate;
                    weight = weight_candidate;
                    block_size = block_size_cantidate;
                    increment_window = false;
                }

                end_candidate = (end + step) < n ? (end + step) : n - 1;
                block_size_cantidate = (end_candidate - begin) + 1;
                weight_candidate = compute_weight(begin_element, list[end_candidate], block_size_cantidate);

                // If the previously computed weight is lesser than the new candidate...
                if (weight_candidate > weight) {
                    // Appends block size to partitions.
                    partitions.push_back(block_size);
                    weights.push_back(weight);

                    // Reinitializes a new window.
                    begin = end + 1;
                    end = begin + step < n ? begin + (step - 1) : n - 1;
                    block_size = (end - begin) + 1;
                    begin_element = list[begin - 1];
                    weight = compute_weight(begin_element, list[end], block_size);
                } else {
                    increment_window = true;
                }

                // Break condition.
                if (end_candidate == end && end == n - 1) {
                    partitions.push_back(block_size);
                    weights.push_back(weight);
                    break;
                }

            } while (true);

            return {partitions, weights};
        }
    };
}