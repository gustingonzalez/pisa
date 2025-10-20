#include "codec/simdbp.hpp"
#include "codec/block_codecs.hpp"
#include "util/util.hpp"
#include <algorithm>
#include <vector>

extern "C" {
#include "simdcomp/include/simdbitpacking.h"
}

namespace pisa {
void SimdBpBlockCodec::encode_128(
    uint32_t const* in, std::vector<uint8_t>& out, size_t n, uint32_t b
) const {
    static thread_local std::vector<uint8_t> buf;
    auto const required = 8 * block_size();
    if (buf.size() < required) {
        buf.resize(required);
    }
    auto* buf_ptr = buf.data();
    simdpackwithoutmask(in, reinterpret_cast<__m128i*>(buf_ptr), b);
    out.insert(out.end(), buf_ptr, buf_ptr + b * sizeof(__m128i));
}

void SimdBpBlockCodec::encode(
    uint32_t const* in, uint32_t sum_of_values, size_t n, std::vector<uint8_t>& out
) const {
    assert(n <= m_block_size);

    // It's required to set the SIMDBlockSize to 256!
    assert(SIMDBlockSize == 256);

    auto* src = const_cast<uint32_t*>(in);
    if (n < m_block_size) {
        interpolative_block::encode(src, sum_of_values, n, out);
        return;
    }
    // Computes and encodes the bits required per element.
    uint32_t b = std::max(maxbits(src), maxbits(src + 128));
    out.push_back(b);

    // Encodes the first 128-items.
    encode_128(src, out, n, b);
    // Encodes the remaining 128-items.
    encode_128(src + 128, out, n, b);
}

uint8_t const*
SimdBpBlockCodec::decode(uint8_t const* in, uint32_t* out, uint32_t sum_of_values, size_t n) const {
    assert(n <= m_block_size);
    if (n < m_block_size) [[unlikely]] {
        return interpolative_block::decode(in, out, sum_of_values, n);
    }
    uint32_t b = *in++;

    // Reads the first 128 items (SIMD-BP works with 128-chunks).
    simdunpack((const __m128i*)in, out, b);

    // Given that in the previous one instruction are readed the first 128
    // items, and being 'b' the bits used per item, it's required to move 'b'
    // bytes to read the remaining 128. Then b = 128b/8 => b = 16b. Also, note
    // that the next two instructions are independents of the previous one,
    // avoiding 'data hazard' on pipelining.
    uint8_t const* second_unpack = in + 16 * b;
    simdunpack((const __m128i*)second_unpack, out += 128, b);

    return second_unpack + 16 * b;
}
}  // namespace pisa
