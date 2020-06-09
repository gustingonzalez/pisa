#pragma once

namespace pisa {
    struct GreedyPartition {

        static size_t compute_weight(uint32_t end, uint32_t block_size) {
            return end / block_size;
        }

        template <typename Iterator>
        static std::vector<uint32_t> compute(Iterator list, uint32_t n, uint32_t step) {
            std::vector<uint32_t> partitions;
            uint32_t last_computed(-1);
            uint32_t list_size = 0;

            // start-end window
            uint32_t start = 0;
            uint32_t end = step < n ? step - 1 : n - 1;
            uint32_t block_size = end - start + 1;
            size_t weight = compute_weight(list[end], block_size);
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
                block_size_cantidate = (end_candidate - start) + 1;
                weight_candidate = compute_weight(list[end_candidate], block_size_cantidate);

                // If the previously computed weight is lesser than the new candidate...
                if (weight_candidate > weight) {
                    // Appends block size to partitions.
                    partitions.push_back(block_size);

                    // Reinitializes a new window.
                    start = end + 1;
                    end = start + step < n ? start + (step - 1) : n - 1;
                    block_size = (end - start) + 1;
                    weight = compute_weight(list[end], block_size);
                } else {
                    increment_window = true;
                }

                // Break condition.
                if (end_candidate == end && end == n - 1) {
                    partitions.push_back(block_size);
                    break;
                }

            } while(true);

            return partitions;
        }
    };
}