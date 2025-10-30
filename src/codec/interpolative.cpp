#include <cassert>

#include "codec/block_codecs.hpp"
#include "codec/interpolative.hpp"

namespace pisa {

void InterpolativeBlockCodec::encode(
    uint32_t const* in, uint32_t sum_of_values, size_t n, std::vector<uint8_t>& out
) const {
    interpolative_block::encode(in, sum_of_values, n, out);
}

uint8_t const* InterpolativeBlockCodec::decode(
    uint8_t const* in, uint32_t* out, uint32_t sum_of_values, size_t n
) const {
    return interpolative_block::decode(in, out, sum_of_values, n);
}

size_t InterpolativeBlockCodec::compute_encoded_size(
    uint32_t const* in, uint32_t sum_of_values, size_t n
) {
    return interpolative_block::compute_encoded_size(in, sum_of_values, n);
}

}  // namespace pisa
