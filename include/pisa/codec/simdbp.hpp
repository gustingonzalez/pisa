#pragma once

#include <vector>
#include "util/util.hpp"
#include "codec/block_codecs.hpp"

extern "C" {
#include "simdcomp/include/simdbitpacking.h"
}

namespace pisa {
struct simdbp_block {
    static const uint64_t block_size = 64;
    static void encode(uint32_t const *in,
                       uint32_t sum_of_values,
                       size_t n,
                       std::vector<uint8_t> &out) {

        assert(n <= block_size);
        uint32_t *src = const_cast<uint32_t *>(in);
        uint32_t b = maxbits(in);
        thread_local std::vector<uint8_t> buf(8*n);
        uint8_t * buf_ptr = buf.data();
        *buf_ptr++ = b;
        simdpackwithoutmask(src, (__m128i *)buf_ptr, b);
        out.insert(out.end(), buf.data(), buf.data() + b * sizeof(__m128i) + 1);
    }
    static uint8_t const *decode(uint8_t const *in,
                                 uint32_t *out,
                                 uint32_t sum_of_values,
                                 size_t n) {
        assert(n <= block_size);
        uint32_t b = *in++;
        simdunpack((const __m128i *)in, out, b);
        return in +  b * sizeof(__m128i);
    }
};
} // namespace pisa