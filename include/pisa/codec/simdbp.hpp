#pragma once

#include <vector>
#include "util/util.hpp"
#include "codec/block_codecs.hpp"

extern "C" {
#include "simdcomp/include/simdbitpacking.h"
}

namespace pisa {
struct simdbp_block {
   static const uint64_t block_size = 256;
 
   static void encode_128(uint32_t const *in, std::vector<uint8_t> &out, size_t n, uint32_t b) {
       thread_local std::vector<uint8_t> buf(8*n);
       uint8_t * buf_ptr = buf.data();
       simdpackwithoutmask(in, (__m128i *)buf_ptr, b);
       out.insert(out.end(), buf.data(), buf.data() + b * sizeof(__m128i));
   }
 
   static void encode(uint32_t const *in,
                      uint32_t sum_of_values,
                      size_t n,
                      std::vector<uint8_t> &out) {
       assert(n <= block_size);
       uint32_t *src = const_cast<uint32_t *>(in);
       // It's required to set the SIMDBlockSize to 256!
       assert(SIMDBlockSize == 256);

       // Computes and encodes the bits required per element.
       uint8_t b = maxbits(in);
       out.push_back(b);

       // Encodes the first 128-items.
       encode_128(in, out, n, b);
       // Encodes the remaining 128-items.
       encode_128(in+=128, out, n, b);
   }
 
    static uint8_t const *decode_128(uint8_t const *in,
                                uint32_t *out,
                                uint32_t b)
   {
       simdunpack((const __m128i *)in, out, b);
       return in +  b * sizeof(__m128i);
   }
 
   static uint8_t const *decode(uint8_t const *in,
                                uint32_t *out,
                                uint32_t sum_of_values,
                                size_t n) {
       assert(n <= block_size);
       uint32_t b = *in++;
 
       // Reads the first 128 items (SIMBP works with 128-chunks).
       simdunpack((const __m128i *)in, out, b);
 
       // Given that in the previous one instruction are readed the first 128 items,
       // and being 'b' the bits used per item, it's required to move 'b' bytes to
       // read the remaining 128. Then b = 128b/8 => b = 16b. Also, note that the
       // next two instructions are independents of the previous one, avoiding 'data
       // hazard' on pipelining.
       uint8_t const * second_unpack = in + 16*b;
       simdunpack((const __m128i *)second_unpack, out+=128, b);
 
       return second_unpack + 16*b;
   }
};
} // namespace pisa